/*
 * #%L
 * HAPI FHIR JPA Server
 * %%
 * Copyright (C) 2014 - 2023 Smile CDR, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package ca.uhn.fhir.jpa.dao;

import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.i18n.Msg;
import ca.uhn.fhir.interceptor.api.HookParams;
import ca.uhn.fhir.interceptor.api.IInterceptorBroadcaster;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.interceptor.model.RequestPartitionId;
import ca.uhn.fhir.jpa.api.config.JpaStorageSettings;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.api.model.DaoMethodOutcome;
import ca.uhn.fhir.jpa.api.svc.IIdHelperService;
import ca.uhn.fhir.jpa.dao.index.IdHelperService;
import ca.uhn.fhir.jpa.model.cross.IBasePersistedResource;
import ca.uhn.fhir.jpa.model.dao.JpaPid;
import ca.uhn.fhir.jpa.model.entity.BaseHasResource;
import ca.uhn.fhir.jpa.model.entity.BaseTag;
import ca.uhn.fhir.jpa.model.entity.ForcedId;
import ca.uhn.fhir.jpa.model.entity.PartitionablePartitionId;
import ca.uhn.fhir.jpa.model.entity.ResourceHistoryTable;
import ca.uhn.fhir.jpa.model.entity.ResourceTable;
import ca.uhn.fhir.jpa.model.entity.TagDefinition;
import ca.uhn.fhir.jpa.model.entity.TagTypeEnum;
import ca.uhn.fhir.jpa.model.util.JpaConstants;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.api.InterceptorInvocationTimingEnum;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.RestOperationTypeEnum;
import ca.uhn.fhir.rest.api.ValidationModeEnum;
import ca.uhn.fhir.rest.api.server.IPreResourceAccessDetails;
import ca.uhn.fhir.rest.api.server.IPreResourceShowDetails;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.api.server.SimplePreResourceAccessDetails;
import ca.uhn.fhir.rest.api.server.SimplePreResourceShowDetails;
import ca.uhn.fhir.rest.api.server.SystemRequestDetails;
import ca.uhn.fhir.rest.api.server.storage.IResourcePersistentId;
import ca.uhn.fhir.rest.api.server.storage.TransactionDetails;
import ca.uhn.fhir.rest.server.IPagingProvider;
import ca.uhn.fhir.rest.server.IRestfulServerDefaults;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import ca.uhn.fhir.rest.server.util.CompositeInterceptorBroadcaster;
import ca.uhn.fhir.util.ReflectionUtil;
import ca.uhn.fhir.util.StopWatch;
import ca.uhn.fhir.validation.FhirValidator;
import ca.uhn.fhir.validation.IInstanceValidatorModule;
import ca.uhn.fhir.validation.IValidationContext;
import ca.uhn.fhir.validation.IValidatorModule;
import ca.uhn.fhir.validation.ValidationOptions;
import ca.uhn.fhir.validation.ValidationResult;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.NoResultException;
import jakarta.persistence.TypedQuery;
import jakarta.transaction.Transactional;
import org.apache.commons.lang3.Validate;
import org.hl7.fhir.instance.model.api.IBaseCoding;
import org.hl7.fhir.instance.model.api.IBaseMetaType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public abstract class BaseHapiFhirResourceDao<T extends IBaseResource> extends BaseHapiFhirDao<T>
		implements IFhirResourceDao<T> {

	public static final String BASE_RESOURCE_NAME = "resource";
	private static final org.slf4j.Logger ourLog = org.slf4j.LoggerFactory.getLogger(BaseHapiFhirResourceDao.class);

	private IInstanceValidatorModule myInstanceValidator;
	private String myResourceName;
	private Class<T> myResourceType;

	public static <T extends IBaseResource> T invokeStoragePreShowResources(
			IInterceptorBroadcaster theInterceptorBroadcaster, RequestDetails theRequest, T retVal) {
		if (CompositeInterceptorBroadcaster.hasHooks(
				Pointcut.STORAGE_PRESHOW_RESOURCES, theInterceptorBroadcaster, theRequest)) {
			SimplePreResourceShowDetails showDetails = new SimplePreResourceShowDetails(retVal);
			HookParams params = new HookParams()
					.add(IPreResourceShowDetails.class, showDetails)
					.add(RequestDetails.class, theRequest)
					.addIfMatchesType(ServletRequestDetails.class, theRequest);
			CompositeInterceptorBroadcaster.doCallHooks(
					theInterceptorBroadcaster, theRequest, Pointcut.STORAGE_PRESHOW_RESOURCES, params);
			//noinspection unchecked
			retVal = (T) showDetails.getResource(
					0); // TODO GGG/JA : getting resource 0 is interesting. We apparently allow null values in the list.
			// Should we?
			return retVal;
		} else {
			return retVal;
		}
	}

	public static void invokeStoragePreAccessResources(
			IInterceptorBroadcaster theInterceptorBroadcaster,
			RequestDetails theRequest,
			IIdType theId,
			IBaseResource theResource) {
		if (CompositeInterceptorBroadcaster.hasHooks(
				Pointcut.STORAGE_PREACCESS_RESOURCES, theInterceptorBroadcaster, theRequest)) {
			SimplePreResourceAccessDetails accessDetails = new SimplePreResourceAccessDetails(theResource);
			HookParams params = new HookParams()
					.add(IPreResourceAccessDetails.class, accessDetails)
					.add(RequestDetails.class, theRequest)
					.addIfMatchesType(ServletRequestDetails.class, theRequest);
			CompositeInterceptorBroadcaster.doCallHooks(
					theInterceptorBroadcaster, theRequest, Pointcut.STORAGE_PREACCESS_RESOURCES, params);
			if (accessDetails.isDontReturnResourceAtIndex(0)) {
				throw new ResourceNotFoundException(Msg.code(1995) + "Resource " + theId + " is not known");
			}
		}
	}

	@Override
	protected IStorageResourceParser getStorageResourceParser() {
		return myJpaStorageResourceParser;
	}

	/**
	 * @deprecated Use {@link #create(T, RequestDetails)} instead
	 */
	@Transactional
	@Override
	public DaoMethodOutcome create(final T theResource) {
		return create(theResource, null, true, null, new TransactionDetails());
	}

	@Override
	public DaoMethodOutcome create(final T theResource, RequestDetails theRequestDetails) {
		return create(theResource, null, true, theRequestDetails, new TransactionDetails());
	}

	/**
	 * @deprecated Use {@link #create(T, String, RequestDetails)} instead
	 */
	@Override
	public DaoMethodOutcome create(final T theResource, String theIfNoneExist) {
		return create(theResource, theIfNoneExist, null);
	}

	@Override
	public DaoMethodOutcome create(final T theResource, String theIfNoneExist, RequestDetails theRequestDetails) {
		return create(theResource, theIfNoneExist, true, theRequestDetails, new TransactionDetails());
	}

	@Override
	public DaoMethodOutcome create(
			T theResource,
			String theIfNoneExist,
			boolean thePerformIndexing,
			RequestDetails theRequestDetails,
			@Nonnull TransactionDetails theTransactionDetails) {
		RequestPartitionId requestPartitionId = RequestPartitionId.allPartitions();
		//		return myTransactionService
		//			.withRequest(theRequestDetails)
		//			.withTransactionDetails(theTransactionDetails)
		//			.withRequestPartitionId(requestPartitionId)
		//			.execute(tx -> doCreateForPost(
		//				theResource,
		//				theIfNoneExist,
		//				thePerformIndexing,
		//				theTransactionDetails,
		//				theRequestDetails,
		//				requestPartitionId));
		// use @Transactional rather than this
		DaoMethodOutcome retVal = doCreateForPost(
				theResource,
				theIfNoneExist,
				thePerformIndexing,
				theTransactionDetails,
				theRequestDetails,
				requestPartitionId);
		return retVal;
	}

	/**
	 * Called for FHIR create (POST) operations
	 */
	protected DaoMethodOutcome doCreateForPost(
			T theResource,
			String theIfNoneExist,
			boolean thePerformIndexing,
			TransactionDetails theTransactionDetails,
			RequestDetails theRequestDetails,
			RequestPartitionId theRequestPartitionId) {
		if (theResource == null) {
			String msg = getContext().getLocalizer().getMessage(BaseStorageDao.class, "missingBody");
			throw new InvalidRequestException(Msg.code(956) + msg);
		}

		if (isNotBlank(theResource.getIdElement().getIdPart())) {
			if (getContext().getVersion().getVersion().isOlderThan(FhirVersionEnum.DSTU3)) {
				String message = getMessageSanitized(
						"failedToCreateWithClientAssignedId",
						theResource.getIdElement().getIdPart());
				throw new InvalidRequestException(
						Msg.code(957) + message, createErrorOperationOutcome(message, "processing"));
			} else {
				// As of DSTU3, ID and version in the body should be ignored for a create/update
				theResource.setId("");
			}
		}

		if (getStorageSettings().getResourceServerIdStrategy() == JpaStorageSettings.IdStrategyEnum.UUID) {
			theResource.setId(UUID.randomUUID().toString());
			theResource.setUserData(JpaConstants.RESOURCE_ID_SERVER_ASSIGNED, Boolean.TRUE);
		}

		return doCreateForPostOrPut(
				theRequestDetails,
				theResource,
				theIfNoneExist,
				true,
				thePerformIndexing,
				theRequestPartitionId,
				RestOperationTypeEnum.CREATE,
				theTransactionDetails);
	}

	/**
	 * Called both for FHIR create (POST) operations (via {@link #doCreateForPost(IBaseResource, String, boolean, TransactionDetails, RequestDetails, RequestPartitionId)}
	 * as well as for FHIR update (PUT) where we're doing a create-with-client-assigned-ID (via {@link #doUpdate(IBaseResource, String, boolean, boolean, RequestDetails, TransactionDetails, RequestPartitionId)}.
	 */
	private DaoMethodOutcome doCreateForPostOrPut(
			RequestDetails theRequest,
			T theResource,
			String theMatchUrl,
			boolean theProcessMatchUrl,
			boolean thePerformIndexing,
			RequestPartitionId theRequestPartitionId,
			RestOperationTypeEnum theOperationType,
			TransactionDetails theTransactionDetails) {
		StopWatch w = new StopWatch();

		preProcessResourceForStorage(theResource);
		preProcessResourceForStorage(theResource, theRequest, theTransactionDetails, thePerformIndexing);

		ResourceTable entity = new ResourceTable();
		entity.setResourceType(toResourceName(theResource));
		entity.setPartitionId(PartitionablePartitionId.toStoragePartition(theRequestPartitionId, myPartitionSettings));
		entity.setCreatedByMatchUrl(theMatchUrl);
		entity.initializeVersion();

		if (isNotBlank(theMatchUrl) && theProcessMatchUrl) {
			throw new RuntimeException("not implemented");
		}

		String resourceIdBeforeStorage = theResource.getIdElement().getIdPart();
		boolean resourceHadIdBeforeStorage = isNotBlank(resourceIdBeforeStorage);
		boolean resourceIdWasServerAssigned =
				theResource.getUserData(JpaConstants.RESOURCE_ID_SERVER_ASSIGNED) == Boolean.TRUE;
		if (resourceHadIdBeforeStorage) {
			entity.setFhirId(resourceIdBeforeStorage);
		}

		HookParams hookParams;

		// Notify interceptor for accepting/rejecting client assigned ids
		if (!resourceIdWasServerAssigned && resourceHadIdBeforeStorage) {
			hookParams = new HookParams().add(IBaseResource.class, theResource).add(RequestDetails.class, theRequest);
			doCallHooks(theTransactionDetails, theRequest, Pointcut.STORAGE_PRESTORAGE_CLIENT_ASSIGNED_ID, hookParams);
		}

		// Interceptor call: STORAGE_PRESTORAGE_RESOURCE_CREATED
		hookParams = new HookParams()
				.add(IBaseResource.class, theResource)
				.add(RequestDetails.class, theRequest)
				.addIfMatchesType(ServletRequestDetails.class, theRequest)
				.add(RequestPartitionId.class, theRequestPartitionId)
				.add(TransactionDetails.class, theTransactionDetails);
		doCallHooks(theTransactionDetails, theRequest, Pointcut.STORAGE_PRESTORAGE_RESOURCE_CREATED, hookParams);

		if (resourceHadIdBeforeStorage && !resourceIdWasServerAssigned) {
			validateResourceIdCreation(theResource, theRequest);
		}

		if (theMatchUrl != null) {
			// Note: We actually create the search URL below by calling enforceMatchUrlResourceUniqueness
			// since we can't do that until we know the assigned PID, but we set this flag up here
			// because we need to set it before we persist the ResourceTable entity in order to
			// avoid triggering an extra DB update
			entity.setSearchUrlPresent(true);
		}

		// Perform actual DB update
		// this call will also update the metadata
		ResourceTable updatedEntity = updateEntity(
				theRequest,
				theResource,
				entity,
				null,
				thePerformIndexing,
				false,
				theTransactionDetails,
				false,
				thePerformIndexing);

		// Store the resource forced ID if necessary
		JpaPid jpaPid = JpaPid.fromId(updatedEntity.getResourceId());
		if (resourceHadIdBeforeStorage) {
			if (resourceIdWasServerAssigned) {
				boolean createForPureNumericIds = true;
				createForcedIdIfNeeded(entity, resourceIdBeforeStorage, createForPureNumericIds);
			} else {
				boolean createForPureNumericIds = getStorageSettings().getResourceClientIdStrategy()
						!= JpaStorageSettings.ClientIdStrategyEnum.ALPHANUMERIC;
				createForcedIdIfNeeded(entity, resourceIdBeforeStorage, createForPureNumericIds);
			}
		} else {
			switch (getStorageSettings().getResourceClientIdStrategy()) {
				case NOT_ALLOWED:
				case ALPHANUMERIC:
					break;
				case ANY:
					boolean createForPureNumericIds = true;
					createForcedIdIfNeeded(
							updatedEntity, theResource.getIdElement().getIdPart(), createForPureNumericIds);
					// for client ID mode ANY, we will always have a forced ID. If we ever
					// stop populating the transient forced ID be warned that we use it
					// (and expect it to be set correctly) farther below.
					assert updatedEntity.getTransientForcedId() != null;
					break;
			}
		}

		// Populate the resource with its actual final stored ID from the entity
		theResource.setId(entity.getIdDt());

		// Pre-cache the resource ID
		jpaPid.setAssociatedResourceId(entity.getIdType(myFhirContext));
		myIdHelperService.addResolvedPidToForcedId(
				jpaPid, theRequestPartitionId, getResourceName(), entity.getTransientForcedId(), null);
		theTransactionDetails.addResolvedResourceId(jpaPid.getAssociatedResourceId(), jpaPid);
		theTransactionDetails.addResolvedResource(jpaPid.getAssociatedResourceId(), theResource);

		// Pre-cache the match URL, and create an entry in the HFJ_RES_SEARCH_URL table to
		// protect against concurrent writes to the same conditional URL
		if (theMatchUrl != null) {
			throw new RuntimeException("not implemented");
		}

		// Update the version/last updated in the resource so that interceptors get
		// the correct version
		// TODO - the above updateEntity calls updateResourceMetadata
		// 		Maybe we don't need this call here?
		myJpaStorageResourceParser.updateResourceMetadata(entity, theResource);

		// Populate the PID in the resource so it is available to hooks
		addPidToResource(entity, theResource);

		// Notify JPA interceptors
		if (!updatedEntity.isUnchangedInCurrentOperation()) {
			hookParams = new HookParams()
					.add(IBaseResource.class, theResource)
					.add(RequestDetails.class, theRequest)
					.addIfMatchesType(ServletRequestDetails.class, theRequest)
					.add(TransactionDetails.class, theTransactionDetails)
					.add(
							InterceptorInvocationTimingEnum.class,
							theTransactionDetails.getInvocationTiming(Pointcut.STORAGE_PRECOMMIT_RESOURCE_CREATED));
			doCallHooks(theTransactionDetails, theRequest, Pointcut.STORAGE_PRECOMMIT_RESOURCE_CREATED, hookParams);
		}

		DaoMethodOutcome outcome = toMethodOutcome(theRequest, entity, theResource, theMatchUrl, theOperationType)
				.setCreated(true);

		if (!thePerformIndexing) {
			outcome.setId(theResource.getIdElement());
		}

		populateOperationOutcomeForUpdate(w, outcome, theMatchUrl, theOperationType);

		return outcome;
	}

	private void createForcedIdIfNeeded(
			ResourceTable theEntity, String theResourceId, boolean theCreateForPureNumericIds) {
		if (isNotBlank(theResourceId) && theEntity.getForcedId() == null) {
			if (theCreateForPureNumericIds || !IdHelperService.isValidPid(theResourceId)) {
				ForcedId forcedId = new ForcedId();
				forcedId.setResourceType(theEntity.getResourceType());
				forcedId.setForcedId(theResourceId);
				forcedId.setResource(theEntity);
				forcedId.setPartitionId(theEntity.getPartitionId());

				/*
				 * As of Hibernate 5.6.2, assigning the forced ID to the
				 * resource table causes an extra update to happen, even
				 * though the ResourceTable entity isn't actually changed
				 * (there is a @OneToOne reference on ResourceTable to the
				 * ForcedId table, but the actual column is on the ForcedId
				 * table so it doesn't actually make sense to update the table
				 * when this is set). But to work around that we avoid
				 * actually assigning ResourceTable#myForcedId here.
				 *
				 * It's conceivable they may fix this in the future, or
				 * they may not.
				 *
				 * If you want to try assigning the forced it to the resource
				 * entity (by calling ResourceTable#setForcedId) try running
				 * the tests FhirResourceDaoR4QueryCountTest to verify that
				 * nothing has broken as a result.
				 * JA 20220121
				 */
				theEntity.setTransientForcedId(forcedId.getForcedId());
				myForcedIdDao.save(forcedId);
			}
		}
	}

	void validateResourceIdCreation(T theResource, RequestDetails theRequest) {
		JpaStorageSettings.ClientIdStrategyEnum strategy = getStorageSettings().getResourceClientIdStrategy();

		if (strategy == JpaStorageSettings.ClientIdStrategyEnum.NOT_ALLOWED) {
			if (!isSystemRequest(theRequest)) {
				throw new ResourceNotFoundException(Msg.code(959)
						+ getMessageSanitized(
								"failedToCreateWithClientAssignedIdNotAllowed",
								theResource.getIdElement().getIdPart()));
			}
		}

		if (strategy == JpaStorageSettings.ClientIdStrategyEnum.ALPHANUMERIC) {
			if (theResource.getIdElement().isIdPartValidLong()) {
				throw new InvalidRequestException(Msg.code(960)
						+ getMessageSanitized(
								"failedToCreateWithClientAssignedNumericId",
								theResource.getIdElement().getIdPart()));
			}
		}
	}

	protected String getMessageSanitized(String theKey, String theIdPart) {
		return getContext().getLocalizer().getMessageSanitized(BaseStorageDao.class, theKey, theIdPart);
	}

	private boolean isSystemRequest(RequestDetails theRequest) {
		return theRequest instanceof SystemRequestDetails;
	}

	private IInstanceValidatorModule getInstanceValidator() {
		return myInstanceValidator;
	}

	private <MT extends IBaseMetaType> void doMetaAdd(
			MT theMetaAdd,
			BaseHasResource theEntity,
			RequestDetails theRequestDetails,
			TransactionDetails theTransactionDetails) {
		IBaseResource oldVersion = myJpaStorageResourceParser.toResource(theEntity, false);

		List<TagDefinition> tags = toTagList(theMetaAdd);
		for (TagDefinition nextDef : tags) {

			boolean hasTag = false;
			for (BaseTag next : new ArrayList<>(theEntity.getTags())) {
				if (Objects.equals(next.getTag().getTagType(), nextDef.getTagType())
						&& Objects.equals(next.getTag().getSystem(), nextDef.getSystem())
						&& Objects.equals(next.getTag().getCode(), nextDef.getCode())
						&& Objects.equals(next.getTag().getVersion(), nextDef.getVersion())
						&& Objects.equals(next.getTag().getUserSelected(), nextDef.getUserSelected())) {
					hasTag = true;
					break;
				}
			}

			if (!hasTag) {
				theEntity.setHasTags(true);

				TagDefinition def = getTagOrNull(
						theTransactionDetails,
						nextDef.getTagType(),
						nextDef.getSystem(),
						nextDef.getCode(),
						nextDef.getDisplay(),
						nextDef.getVersion(),
						nextDef.getUserSelected());
				if (def != null) {
					BaseTag newEntity = theEntity.addTag(def);
					if (newEntity.getTagId() == null) {
						myEntityManager.persist(newEntity);
					}
				}
			}
		}

		validateMetaCount(theEntity.getTags().size());

		myEntityManager.merge(theEntity);

		// Interceptor call: STORAGE_PRECOMMIT_RESOURCE_UPDATED
		IBaseResource newVersion = myJpaStorageResourceParser.toResource(theEntity, false);
		HookParams preStorageParams = new HookParams()
				.add(IBaseResource.class, oldVersion)
				.add(IBaseResource.class, newVersion)
				.add(RequestDetails.class, theRequestDetails)
				.addIfMatchesType(ServletRequestDetails.class, theRequestDetails)
				.add(TransactionDetails.class, theTransactionDetails);
		// myInterceptorBroadcaster.callHooks(Pointcut.STORAGE_PRESTORAGE_RESOURCE_UPDATED, preStorageParams);

		// Interceptor call: STORAGE_PRECOMMIT_RESOURCE_UPDATED
		HookParams preCommitParams = new HookParams()
				.add(IBaseResource.class, oldVersion)
				.add(IBaseResource.class, newVersion)
				.add(RequestDetails.class, theRequestDetails)
				.addIfMatchesType(ServletRequestDetails.class, theRequestDetails)
				.add(TransactionDetails.class, theTransactionDetails)
				.add(
						InterceptorInvocationTimingEnum.class,
						theTransactionDetails.getInvocationTiming(Pointcut.STORAGE_PRECOMMIT_RESOURCE_UPDATED));
		// myInterceptorBroadcaster.callHooks(Pointcut.STORAGE_PRECOMMIT_RESOURCE_UPDATED, preCommitParams);
	}

	private <MT extends IBaseMetaType> void doMetaDelete(
			MT theMetaDel,
			BaseHasResource theEntity,
			RequestDetails theRequestDetails,
			TransactionDetails theTransactionDetails) {

		// todo mb update hibernate search index if we are storing resources - it assumes inline tags.
		IBaseResource oldVersion = myJpaStorageResourceParser.toResource(theEntity, false);

		List<TagDefinition> tags = toTagList(theMetaDel);

		for (TagDefinition nextDef : tags) {
			for (BaseTag next : new ArrayList<BaseTag>(theEntity.getTags())) {
				if (Objects.equals(next.getTag().getTagType(), nextDef.getTagType())
						&& Objects.equals(next.getTag().getSystem(), nextDef.getSystem())
						&& Objects.equals(next.getTag().getCode(), nextDef.getCode())) {
					myEntityManager.remove(next);
					theEntity.getTags().remove(next);
				}
			}
		}

		if (theEntity.getTags().isEmpty()) {
			theEntity.setHasTags(false);
		}

		theEntity = myEntityManager.merge(theEntity);

		// Interceptor call: STORAGE_PRECOMMIT_RESOURCE_UPDATED
		IBaseResource newVersion = myJpaStorageResourceParser.toResource(theEntity, false);
		HookParams preStorageParams = new HookParams()
				.add(IBaseResource.class, oldVersion)
				.add(IBaseResource.class, newVersion)
				.add(RequestDetails.class, theRequestDetails)
				.addIfMatchesType(ServletRequestDetails.class, theRequestDetails)
				.add(TransactionDetails.class, theTransactionDetails);
		// myInterceptorBroadcaster.callHooks(Pointcut.STORAGE_PRESTORAGE_RESOURCE_UPDATED, preStorageParams);

		HookParams preCommitParams = new HookParams()
				.add(IBaseResource.class, oldVersion)
				.add(IBaseResource.class, newVersion)
				.add(RequestDetails.class, theRequestDetails)
				.addIfMatchesType(ServletRequestDetails.class, theRequestDetails)
				.add(TransactionDetails.class, theTransactionDetails)
				.add(
						InterceptorInvocationTimingEnum.class,
						theTransactionDetails.getInvocationTiming(Pointcut.STORAGE_PRECOMMIT_RESOURCE_UPDATED));

		// myInterceptorBroadcaster.callHooks(Pointcut.STORAGE_PRECOMMIT_RESOURCE_UPDATED, preCommitParams);
	}

	@Override
	@Nonnull
	public String getResourceName() {
		return myResourceName;
	}

	@Override
	public Class<T> getResourceType() {
		return myResourceType;
	}

	@SuppressWarnings("unchecked")
	public void setResourceType(Class<? extends IBaseResource> theTableType) {
		myResourceType = (Class<T>) theTableType;
	}

	protected boolean isPagingProviderDatabaseBacked(RequestDetails theRequestDetails) {
		if (theRequestDetails == null || theRequestDetails.getServer() == null) {
			return false;
		}
		IRestfulServerDefaults server = theRequestDetails.getServer();
		IPagingProvider pagingProvider = server.getPagingProvider();
		return pagingProvider != null;
	}

	@Override
	@Transactional
	public <MT extends IBaseMetaType> MT metaAddOperation(
			IIdType theResourceId, MT theMetaAdd, RequestDetails theRequest) {
		TransactionDetails transactionDetails = new TransactionDetails();

		StopWatch w = new StopWatch();
		BaseHasResource entity = readEntity(theResourceId, theRequest);
		if (entity == null) {
			throw new ResourceNotFoundException(Msg.code(1993) + theResourceId);
		}

		ResourceTable latestVersion = readEntityLatestVersion(theResourceId, theRequest, transactionDetails);
		if (latestVersion.getVersion() != entity.getVersion()) {
			doMetaAdd(theMetaAdd, entity, theRequest, transactionDetails);
		} else {
			doMetaAdd(theMetaAdd, latestVersion, theRequest, transactionDetails);

			// Also update history entry
			ResourceHistoryTable history = myResourceHistoryTableDao.findForIdAndVersionAndFetchProvenance(
					entity.getId(), entity.getVersion());
			doMetaAdd(theMetaAdd, history, theRequest, transactionDetails);
		}

		ourLog.debug("Processed metaAddOperation on {} in {}ms", theResourceId, w.getMillisAndRestart());

		@SuppressWarnings("unchecked")
		MT retVal = (MT) metaGetOperation(theMetaAdd.getClass(), theResourceId, theRequest);
		return retVal;
	}

	@Override
	@Transactional
	public <MT extends IBaseMetaType> MT metaDeleteOperation(
			IIdType theResourceId, MT theMetaDel, RequestDetails theRequest) {
		TransactionDetails transactionDetails = new TransactionDetails();

		StopWatch w = new StopWatch();
		BaseHasResource entity = readEntity(theResourceId, theRequest);
		if (entity == null) {
			throw new ResourceNotFoundException(Msg.code(1994) + theResourceId);
		}

		ResourceTable latestVersion = readEntityLatestVersion(theResourceId, theRequest, transactionDetails);
		boolean nonVersionedTags =
				myStorageSettings.getTagStorageMode() != JpaStorageSettings.TagStorageModeEnum.VERSIONED;
		if (latestVersion.getVersion() != entity.getVersion() || nonVersionedTags) {
			doMetaDelete(theMetaDel, entity, theRequest, transactionDetails);
		} else {
			doMetaDelete(theMetaDel, latestVersion, theRequest, transactionDetails);
			// Also update history entry
			ResourceHistoryTable history = myResourceHistoryTableDao.findForIdAndVersionAndFetchProvenance(
					entity.getId(), entity.getVersion());
			doMetaDelete(theMetaDel, history, theRequest, transactionDetails);
		}

		ourLog.debug("Processed metaDeleteOperation on {} in {}ms", theResourceId.getValue(), w.getMillisAndRestart());

		@SuppressWarnings("unchecked")
		MT retVal = (MT) metaGetOperation(theMetaDel.getClass(), theResourceId, theRequest);
		return retVal;
	}

	@Override
	@Transactional
	public <MT extends IBaseMetaType> MT metaGetOperation(Class<MT> theType, IIdType theId, RequestDetails theRequest) {
		Set<TagDefinition> tagDefs = new HashSet<>();
		BaseHasResource entity = readEntity(theId, theRequest);
		for (BaseTag next : entity.getTags()) {
			tagDefs.add(next.getTag());
		}
		MT retVal = toMetaDt(theType, tagDefs);

		retVal.setLastUpdated(entity.getUpdatedDate());
		retVal.setVersionId(Long.toString(entity.getVersion()));

		return retVal;
	}

	@Override
	@Transactional
	public <MT extends IBaseMetaType> MT metaGetOperation(Class<MT> theType, RequestDetails theRequestDetails) {
		String sql =
				"SELECT d FROM TagDefinition d WHERE d.myId IN (SELECT DISTINCT t.myTagId FROM ResourceTag t WHERE t.myResourceType = :res_type)";
		TypedQuery<TagDefinition> q = myEntityManager.createQuery(sql, TagDefinition.class);
		q.setParameter("res_type", myResourceName);
		List<TagDefinition> tagDefinitions = q.getResultList();

		return toMetaDt(theType, tagDefinitions);
	}

	private boolean isDeleted(BaseHasResource entityToUpdate) {
		return entityToUpdate.getDeleted() != null;
	}

	@PostConstruct
	@Override
	public void start() {
		assert getStorageSettings() != null;

		RuntimeResourceDefinition def = getContext().getResourceDefinition(myResourceType);
		myResourceName = def.getName();

		ourLog.debug("Starting resource DAO for type: {}", getResourceName());
		// ian: skipping instance validation support
		super.start();
	}

	/**
	 * Subclasses may override to provide behaviour. Invoked within a delete
	 * transaction with the resource that is about to be deleted.
	 */
	protected void preDelete(T theResourceToDelete, ResourceTable theEntityToDelete, RequestDetails theRequestDetails) {
		// nothing by default
	}

	@Override
	@Transactional
	public T readByPid(IResourcePersistentId thePid) {
		return readByPid(thePid, false);
	}

	@Override
	@Transactional
	public T readByPid(IResourcePersistentId thePid, boolean theDeletedOk) {
		StopWatch w = new StopWatch();
		JpaPid jpaPid = (JpaPid) thePid;

		Optional<ResourceTable> entity = myResourceTableDao.findById(jpaPid.getId());
		if (entity.isEmpty()) {
			throw new ResourceNotFoundException(Msg.code(975) + "No resource found with PID " + jpaPid);
		}
		if (isDeleted(entity.get()) && !theDeletedOk) {
			throw createResourceGoneException(entity.get());
		}

		T retVal = myJpaStorageResourceParser.toResource(myResourceType, entity.get(), null, false);

		ourLog.debug("Processed read on {} in {}ms", jpaPid, w.getMillis());
		return retVal;
	}

	/**
	 * @deprecated Use {@link #read(IIdType, RequestDetails)} instead
	 */
	@Override
	@Transactional
	public T read(IIdType theId) {
		return read(theId, null);
	}

	@Override
	public T read(IIdType theId, RequestDetails theRequestDetails) {
		return read(theId, theRequestDetails, false);
	}

	@Override
	public T read(IIdType theId, RequestDetails theRequest, boolean theDeletedOk) {
		validateResourceTypeAndThrowInvalidRequestException(theId);
		TransactionDetails transactionDetails = new TransactionDetails();

		RequestPartitionId requestPartitionId = RequestPartitionId.allPartitions();

		//		return myTransactionService
		//			.withRequest(theRequest)
		//			.withTransactionDetails(transactionDetails)
		//			.withRequestPartitionId(requestPartitionId)
		//			.execute(() -> doReadInTransaction(theId, theRequest, theDeletedOk, requestPartitionId));
		// instead have @Transactional annotation on caller (line 775)
		return doReadInTransaction(theId, theRequest, theDeletedOk, requestPartitionId);
	}

	private T doReadInTransaction(
			IIdType theId, RequestDetails theRequest, boolean theDeletedOk, RequestPartitionId theRequestPartitionId) {

		StopWatch w = new StopWatch();
		BaseHasResource entity = readEntity(theId, true, theRequest, theRequestPartitionId);
		validateResourceType(entity);

		T retVal = myJpaStorageResourceParser.toResource(myResourceType, entity, null, false);

		if (!theDeletedOk) {
			if (isDeleted(entity)) {
				throw createResourceGoneException(entity);
			}
		}
		ourLog.debug("Processed read on {} in {}ms", theId.getValue(), w.getMillisAndRestart());
		return retVal;
	}

	@Override
	public BaseHasResource readEntity(IIdType theId, RequestDetails theRequest) {
		RequestPartitionId requestPartitionId = RequestPartitionId.allPartitions();
		//		return myTransactionService
		//			.withRequest(theRequest)
		//			.withRequestPartitionId(requestPartitionId)
		//			.execute(() -> readEntity(theId, true, theRequest, requestPartitionId));
		return readEntity(theId, true, theRequest, requestPartitionId);
	}

	private BaseHasResource readEntity(
			IIdType theId,
			boolean theCheckForForcedId,
			RequestDetails theRequest,
			RequestPartitionId requestPartitionId) {
		validateResourceTypeAndThrowInvalidRequestException(theId);

		BaseHasResource entity;
		JpaPid pid = myIdHelperService.resolveResourcePersistentIds(
				requestPartitionId, getResourceName(), theId.getIdPart());
		Set<Integer> readPartitions = null;
		if (requestPartitionId.isAllPartitions()) {
			entity = myEntityManager.find(ResourceTable.class, pid.getId());
		} else {
			readPartitions = requestPartitionId.getPartitionIds().stream()
					.map(t -> t == null ? myPartitionSettings.getDefaultPartitionId() : t)
					.collect(Collectors.toSet());
			if (readPartitions.size() == 1) {
				if (readPartitions.contains(null)) {
					entity = myResourceTableDao
							.readByPartitionIdNull(pid.getId())
							.orElse(null);
				} else {
					entity = myResourceTableDao
							.readByPartitionId(readPartitions.iterator().next(), pid.getId())
							.orElse(null);
				}
			} else {
				if (readPartitions.contains(null)) {
					List<Integer> readPartitionsWithoutNull =
							readPartitions.stream().filter(Objects::nonNull).collect(Collectors.toList());
					entity = myResourceTableDao
							.readByPartitionIdsOrNull(readPartitionsWithoutNull, pid.getId())
							.orElse(null);
				} else {
					entity = myResourceTableDao
							.readByPartitionIds(readPartitions, pid.getId())
							.orElse(null);
				}
			}
		}

		// Verify that the resource is for the correct partition
		if (entity != null && readPartitions != null && entity.getPartitionId() != null) {
			if (!readPartitions.contains(entity.getPartitionId().getPartitionId())) {
				ourLog.debug(
						"Performing a read for PartitionId={} but entity has partition: {}",
						requestPartitionId,
						entity.getPartitionId());
				entity = null;
			}
		}

		if (entity == null) {
			throw new ResourceNotFoundException(Msg.code(1996) + "Resource " + theId + " is not known");
		}

		if (theId.hasVersionIdPart()) {
			if (!theId.isVersionIdPartValidLong()) {
				throw new ResourceNotFoundException(Msg.code(978)
						+ getContext()
								.getLocalizer()
								.getMessageSanitized(
										BaseStorageDao.class,
										"invalidVersion",
										theId.getVersionIdPart(),
										theId.toUnqualifiedVersionless()));
			}
			if (entity.getVersion() != theId.getVersionIdPartAsLong()) {
				entity = null;
			}
		}

		if (entity == null) {
			if (theId.hasVersionIdPart()) {
				TypedQuery<ResourceHistoryTable> q = myEntityManager.createQuery(
						"SELECT t from ResourceHistoryTable t WHERE t.myResourceId = :RID AND t.myResourceType = :RTYP AND t.myResourceVersion = :RVER",
						ResourceHistoryTable.class);
				q.setParameter("RID", pid.getId());
				q.setParameter("RTYP", myResourceName);
				q.setParameter("RVER", theId.getVersionIdPartAsLong());
				try {
					entity = q.getSingleResult();
				} catch (NoResultException e) {
					throw new ResourceNotFoundException(Msg.code(979)
							+ getContext()
									.getLocalizer()
									.getMessageSanitized(
											BaseStorageDao.class,
											"invalidVersion",
											theId.getVersionIdPart(),
											theId.toUnqualifiedVersionless()));
				}
			}
		}

		Validate.notNull(entity);
		validateResourceType(entity);

		if (theCheckForForcedId) {
			validateGivenIdIsAppropriateToRetrieveResource(theId, entity);
		}
		return entity;
	}

	@Override
	protected IBasePersistedResource readEntityLatestVersion(
			IResourcePersistentId thePersistentId,
			RequestDetails theRequestDetails,
			TransactionDetails theTransactionDetails) {
		JpaPid jpaPid = (JpaPid) thePersistentId;
		return myEntityManager.find(ResourceTable.class, jpaPid.getId());
	}

	@Override
	@Nonnull
	protected ResourceTable readEntityLatestVersion(
			IIdType theId, RequestDetails theRequestDetails, TransactionDetails theTransactionDetails) {
		RequestPartitionId requestPartitionId = RequestPartitionId.allPartitions();
		return readEntityLatestVersion(theId, requestPartitionId, theTransactionDetails);
	}

	@Nonnull
	private ResourceTable readEntityLatestVersion(
			IIdType theId,
			@Nonnull RequestPartitionId theRequestPartitionId,
			TransactionDetails theTransactionDetails) {
		validateResourceTypeAndThrowInvalidRequestException(theId);

		JpaPid persistentId = null;
		if (theTransactionDetails != null) {
			if (theTransactionDetails.isResolvedResourceIdEmpty(theId.toUnqualifiedVersionless())) {
				throw new ResourceNotFoundException(Msg.code(1997) + theId);
			}
			if (theTransactionDetails.hasResolvedResourceIds()) {
				persistentId = (JpaPid) theTransactionDetails.getResolvedResourceId(theId);
			}
		}

		if (persistentId == null) {
			persistentId = myIdHelperService.resolveResourcePersistentIds(
					theRequestPartitionId, getResourceName(), theId.getIdPart());
		}

		ResourceTable entity = myEntityManager.find(ResourceTable.class, persistentId.getId());
		if (entity == null) {
			throw new ResourceNotFoundException(Msg.code(1998) + theId);
		}
		validateGivenIdIsAppropriateToRetrieveResource(theId, entity);
		entity.setTransientForcedId(theId.getIdPart());
		return entity;
	}

	@Transactional
	@Override
	public void removeTag(IIdType theId, TagTypeEnum theTagType, String theScheme, String theTerm) {
		removeTag(theId, theTagType, theScheme, theTerm, null);
	}

	@Transactional
	@Override
	public void removeTag(
			IIdType theId, TagTypeEnum theTagType, String theScheme, String theTerm, RequestDetails theRequest) {
		StopWatch w = new StopWatch();
		BaseHasResource entity = readEntity(theId, theRequest);
		if (entity == null) {
			throw new ResourceNotFoundException(Msg.code(1999) + theId);
		}

		for (BaseTag next : new ArrayList<>(entity.getTags())) {
			if (Objects.equals(next.getTag().getTagType(), theTagType)
					&& Objects.equals(next.getTag().getSystem(), theScheme)
					&& Objects.equals(next.getTag().getCode(), theTerm)) {
				myEntityManager.remove(next);
				entity.getTags().remove(next);
			}
		}

		if (entity.getTags().isEmpty()) {
			entity.setHasTags(false);
		}

		myEntityManager.merge(entity);

		ourLog.debug(
				"Processed remove tag {}/{} on {} in {}ms",
				theScheme,
				theTerm,
				theId.getValue(),
				w.getMillisAndRestart());
	}

	protected <MT extends IBaseMetaType> MT toMetaDt(Class<MT> theType, Collection<TagDefinition> tagDefinitions) {
		MT retVal = ReflectionUtil.newInstance(theType);
		for (TagDefinition next : tagDefinitions) {
			switch (next.getTagType()) {
				case PROFILE:
					retVal.addProfile(next.getCode());
					break;
				case SECURITY_LABEL:
					retVal.addSecurity()
							.setSystem(next.getSystem())
							.setCode(next.getCode())
							.setDisplay(next.getDisplay());
					break;
				case TAG:
					retVal.addTag()
							.setSystem(next.getSystem())
							.setCode(next.getCode())
							.setDisplay(next.getDisplay());
					break;
			}
		}
		return retVal;
	}

	private ArrayList<TagDefinition> toTagList(IBaseMetaType theMeta) {
		ArrayList<TagDefinition> retVal = new ArrayList<>();

		for (IBaseCoding next : theMeta.getTag()) {
			retVal.add(new TagDefinition(TagTypeEnum.TAG, next.getSystem(), next.getCode(), next.getDisplay()));
		}
		for (IBaseCoding next : theMeta.getSecurity()) {
			retVal.add(
					new TagDefinition(TagTypeEnum.SECURITY_LABEL, next.getSystem(), next.getCode(), next.getDisplay()));
		}
		for (IPrimitiveType<String> next : theMeta.getProfile()) {
			retVal.add(new TagDefinition(TagTypeEnum.PROFILE, BaseHapiFhirDao.NS_JPA_PROFILE, next.getValue(), null));
		}

		return retVal;
	}

	/**
	 * @deprecated Use {@link #update(T, RequestDetails)} instead
	 */
	@Override
	@Transactional
	public DaoMethodOutcome update(T theResource) {
		return update(theResource, null, null);
	}

	@Override
	public DaoMethodOutcome update(T theResource, RequestDetails theRequestDetails) {
		return update(theResource, null, theRequestDetails);
	}

	/**
	 * @deprecated Use {@link #update(T, String, RequestDetails)} instead
	 */
	@Override
	public DaoMethodOutcome update(T theResource, String theMatchUrl) {
		return update(theResource, theMatchUrl, null);
	}

	@Override
	public DaoMethodOutcome update(T theResource, String theMatchUrl, RequestDetails theRequestDetails) {
		return update(theResource, theMatchUrl, true, theRequestDetails);
	}

	@Override
	public DaoMethodOutcome update(
			T theResource, String theMatchUrl, boolean thePerformIndexing, RequestDetails theRequestDetails) {
		return update(theResource, theMatchUrl, thePerformIndexing, false, theRequestDetails, new TransactionDetails());
	}

	@Override
	public DaoMethodOutcome update(
			T theResource,
			String theMatchUrl,
			boolean thePerformIndexing,
			boolean theForceUpdateVersion,
			RequestDetails theRequest,
			@Nonnull TransactionDetails theTransactionDetails) {
		if (theResource == null) {
			String msg = getContext().getLocalizer().getMessage(BaseStorageDao.class, "missingBody");
			throw new InvalidRequestException(Msg.code(986) + msg);
		}
		if (!theResource.getIdElement().hasIdPart() && isBlank(theMatchUrl)) {
			String type = myFhirContext.getResourceType(theResource);
			String msg = myFhirContext.getLocalizer().getMessage(BaseStorageDao.class, "updateWithNoId", type);
			throw new InvalidRequestException(Msg.code(987) + msg);
		}

		/*
		 * Resource updates will modify/update the version of the resource with the new version. This is generally helpful,
		 * but leads to issues if the transaction is rolled back and retried. So if we do a rollback, we reset the resource
		 * version to what it was.
		 */
		String id = theResource.getIdElement().getValue();
		Runnable onRollback = () -> theResource.getIdElement().setValue(id);

		RequestPartitionId requestPartitionId = RequestPartitionId.allPartitions();

		Callable<DaoMethodOutcome> updateCallback;
		if (myStorageSettings.isUpdateWithHistoryRewriteEnabled()
				&& theRequest != null
				&& theRequest.isRewriteHistory()) {
			updateCallback = () ->
					doUpdateWithHistoryRewrite(theResource, theRequest, theTransactionDetails, requestPartitionId);
		} else {
			updateCallback = () -> doUpdate(
					theResource,
					theMatchUrl,
					thePerformIndexing,
					theForceUpdateVersion,
					theRequest,
					theTransactionDetails,
					requestPartitionId);
		}

		// Used @Transactional
		// Execute the update in a retryable transaction
		//		return myTransactionService
		//			.withRequest(theRequest)
		//			.withTransactionDetails(theTransactionDetails)
		//			.withRequestPartitionId(requestPartitionId)
		//			.onRollback(onRollback)
		//			.execute(updateCallback);
		try {
			return updateCallback.call();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private DaoMethodOutcome doUpdate(
			T theResource,
			String theMatchUrl,
			boolean thePerformIndexing,
			boolean theForceUpdateVersion,
			RequestDetails theRequest,
			TransactionDetails theTransactionDetails,
			RequestPartitionId theRequestPartitionId) {

		preProcessResourceForStorage(theResource);
		preProcessResourceForStorage(theResource, theRequest, theTransactionDetails, thePerformIndexing);

		ResourceTable entity = null;

		IIdType resourceId;
		RestOperationTypeEnum update = RestOperationTypeEnum.UPDATE;
		if (isNotBlank(theMatchUrl)) {
			throw new RuntimeException("not implemented");
		} else {
			/*
			 * Note: resourceId will not be null or empty here, because we
			 * check it and reject requests in
			 * BaseOutcomeReturningMethodBindingWithResourceParam
			 */
			resourceId = theResource.getIdElement();
			assert resourceId != null;
			assert resourceId.hasIdPart();

			boolean create = false;

			if (theRequest != null) {
				String existenceCheck = theRequest.getHeader(JpaConstants.HEADER_UPSERT_EXISTENCE_CHECK);
				if (JpaConstants.HEADER_UPSERT_EXISTENCE_CHECK_DISABLED.equals(existenceCheck)) {
					create = true;
				}
			}

			if (!create) {
				try {
					entity = readEntityLatestVersion(resourceId, theRequestPartitionId, theTransactionDetails);
				} catch (ResourceNotFoundException e) {
					create = true;
				}
			}

			if (create) {
				return doCreateForPostOrPut(
						theRequest,
						theResource,
						null,
						false,
						thePerformIndexing,
						theRequestPartitionId,
						update,
						theTransactionDetails);
			}
		}

		// Start
		return doUpdateForUpdateOrPatch(
				theRequest,
				resourceId,
				theMatchUrl,
				thePerformIndexing,
				theForceUpdateVersion,
				theResource,
				entity,
				update,
				theTransactionDetails);
	}

	@Override
	protected DaoMethodOutcome doUpdateForUpdateOrPatch(
			RequestDetails theRequest,
			IIdType theResourceId,
			String theMatchUrl,
			boolean thePerformIndexing,
			boolean theForceUpdateVersion,
			T theResource,
			IBasePersistedResource theEntity,
			RestOperationTypeEnum theOperationType,
			TransactionDetails theTransactionDetails) {

		// we stored a resource searchUrl at creation time to prevent resource duplication.  Let's remove the entry on
		// the
		// first update but guard against unnecessary trips to the database on subsequent ones.
		ResourceTable entity = (ResourceTable) theEntity;

		return super.doUpdateForUpdateOrPatch(
				theRequest,
				theResourceId,
				theMatchUrl,
				thePerformIndexing,
				theForceUpdateVersion,
				theResource,
				theEntity,
				theOperationType,
				theTransactionDetails);
	}

	/**
	 * Method for updating the historical version of the resource when a history version id is included in the request.
	 *
	 * @param theResource           to be saved
	 * @param theRequest            details of the request
	 * @param theTransactionDetails details of the transaction
	 * @return the outcome of the operation
	 */
	private DaoMethodOutcome doUpdateWithHistoryRewrite(
			T theResource,
			RequestDetails theRequest,
			TransactionDetails theTransactionDetails,
			RequestPartitionId theRequestPartitionId) {
		StopWatch w = new StopWatch();

		// No need for indexing as this will update a non-current version of the resource which will not be searchable
		preProcessResourceForStorage(theResource, theRequest, theTransactionDetails, false);

		BaseHasResource entity;
		BaseHasResource currentEntity;

		IIdType resourceId;

		resourceId = theResource.getIdElement();
		assert resourceId != null;
		assert resourceId.hasIdPart();

		try {
			currentEntity =
					readEntityLatestVersion(resourceId.toVersionless(), theRequestPartitionId, theTransactionDetails);

			if (!resourceId.hasVersionIdPart()) {
				throw new InvalidRequestException(
						Msg.code(2093) + "Invalid resource ID, ID must contain a history version");
			}
			entity = readEntity(resourceId, theRequest);
			validateResourceType(entity);
		} catch (ResourceNotFoundException e) {
			throw new ResourceNotFoundException(
					Msg.code(2087) + "Resource not found [" + resourceId + "] - Doesn't exist");
		}

		if (resourceId.hasResourceType() && !resourceId.getResourceType().equals(getResourceName())) {
			throw new UnprocessableEntityException(
					Msg.code(2088) + "Invalid resource ID[" + entity.getIdDt().toUnqualifiedVersionless() + "] of type["
							+ entity.getResourceType() + "] - Does not match expected [" + getResourceName() + "]");
		}
		assert resourceId.hasVersionIdPart();

		boolean wasDeleted = isDeleted(entity);
		entity.setDeleted(null);
		boolean isUpdatingCurrent = resourceId.hasVersionIdPart()
				&& Long.parseLong(resourceId.getVersionIdPart()) == currentEntity.getVersion();
		IBasePersistedResource<?> savedEntity = updateHistoryEntity(
				theRequest, theResource, currentEntity, entity, resourceId, theTransactionDetails, isUpdatingCurrent);
		DaoMethodOutcome outcome = toMethodOutcome(
						theRequest, savedEntity, theResource, null, RestOperationTypeEnum.UPDATE)
				.setCreated(wasDeleted);

		populateOperationOutcomeForUpdate(w, outcome, null, RestOperationTypeEnum.UPDATE);

		return outcome;
	}

	@Override
	@Transactional(Transactional.TxType.SUPPORTS)
	public MethodOutcome validate(
			T theResource,
			IIdType theId,
			String theRawResource,
			EncodingEnum theEncoding,
			ValidationModeEnum theMode,
			String theProfile,
			RequestDetails theRequest) {
		TransactionDetails transactionDetails = new TransactionDetails();

		if (theMode == ValidationModeEnum.DELETE) {
			throw new RuntimeException("not implemented - DELETE");
		}

		FhirValidator validator = getContext().newValidator();
		validator.registerValidatorModule(getInstanceValidator());
		validator.registerValidatorModule(new IdChecker(theMode));

		IBaseResource resourceToValidateById = null;
		if (theId != null && theId.hasResourceType() && theId.hasIdPart()) {
			Class<? extends IBaseResource> type =
					getContext().getResourceDefinition(theId.getResourceType()).getImplementingClass();
			IFhirResourceDao<? extends IBaseResource> dao = myDaoRegistry.getResourceDaoOrNull(type);
			resourceToValidateById = dao.read(theId, theRequest);
		}

		ValidationResult result;
		ValidationOptions options = new ValidationOptions().addProfileIfNotBlank(theProfile);

		if (theResource == null) {
			if (resourceToValidateById != null) {
				result = validator.validateWithResult(resourceToValidateById, options);
			} else {
				String msg = getContext().getLocalizer().getMessage(BaseStorageDao.class, "cantValidateWithNoResource");
				throw new InvalidRequestException(Msg.code(992) + msg);
			}
		} else if (isNotBlank(theRawResource)) {
			result = validator.validateWithResult(theRawResource, options);
		} else {
			result = validator.validateWithResult(theResource, options);
		}

		MethodOutcome retVal = new MethodOutcome();
		retVal.setOperationOutcome(result.toOperationOutcome());
		// Note an earlier version of this code returned PreconditionFailedException when the validation
		// failed, but we since realized the spec requires we return 200 regardless of the validation result.
		return retVal;
	}

	/**
	 * Get the resource definition from the criteria which specifies the resource type
	 */
	@Override
	public RuntimeResourceDefinition validateCriteriaAndReturnResourceDefinition(String criteria) {
		String resourceName;
		if (criteria == null || criteria.trim().isEmpty()) {
			throw new IllegalArgumentException(Msg.code(994) + "Criteria cannot be empty");
		}
		if (criteria.contains("?")) {
			resourceName = criteria.substring(0, criteria.indexOf("?"));
		} else {
			resourceName = criteria;
		}

		return getContext().getResourceDefinition(resourceName);
	}

	private void validateGivenIdIsAppropriateToRetrieveResource(IIdType theId, BaseHasResource entity) {
		if (entity.getForcedId() != null) {
			if (getStorageSettings().getResourceClientIdStrategy() != JpaStorageSettings.ClientIdStrategyEnum.ANY) {
				if (theId.isIdPartValidLong()) {
					// This means that the resource with the given numeric ID exists, but it has a "forced ID", meaning
					// that
					// as far as the outside world is concerned, the given ID doesn't exist (it's just an internal
					// pointer
					// to the
					// forced ID)
					throw new ResourceNotFoundException(Msg.code(2000) + theId);
				}
			}
		}
	}

	private void validateResourceType(BaseHasResource entity) {
		validateResourceType(entity, myResourceName);
	}

	private void validateResourceTypeAndThrowInvalidRequestException(IIdType theId) {
		if (theId.hasResourceType() && !theId.getResourceType().equals(myResourceName)) {
			// Note- Throw a HAPI FHIR exception here so that hibernate doesn't try to translate it into a database
			// exception
			throw new InvalidRequestException(Msg.code(996) + "Incorrect resource type (" + theId.getResourceType()
					+ ") for this DAO, wanted: " + myResourceName);
		}
	}

	@VisibleForTesting
	public void setIdHelperSvcForUnitTest(IIdHelperService theIdHelperService) {
		myIdHelperService = theIdHelperService;
	}

	private static class IdChecker implements IValidatorModule {

		private final ValidationModeEnum myMode;

		IdChecker(ValidationModeEnum theMode) {
			myMode = theMode;
		}

		@Override
		public void validateResource(IValidationContext<IBaseResource> theCtx) {
			IBaseResource resource = theCtx.getResource();
			if (resource instanceof Parameters) {
				List<ParametersParameterComponent> params = ((Parameters) resource).getParameter();
				params = params.stream()
						.filter(param -> param.getName().contains("resource"))
						.collect(Collectors.toList());
				resource = params.get(0).getResource();
			}
			boolean hasId = resource.getIdElement().hasIdPart();
			if (myMode == ValidationModeEnum.CREATE) {
				if (hasId) {
					throw new UnprocessableEntityException(
							Msg.code(997) + "Resource has an ID - ID must not be populated for a FHIR create");
				}
			} else if (myMode == ValidationModeEnum.UPDATE) {
				if (!hasId) {
					throw new UnprocessableEntityException(
							Msg.code(998) + "Resource has no ID - ID must be populated for a FHIR update");
				}
			}
		}
	}
}
