/*-
 * #%L
 * HAPI FHIR Storage api
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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.i18n.Msg;
import ca.uhn.fhir.interceptor.api.HookParams;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.jpa.api.config.JpaStorageSettings;
import ca.uhn.fhir.jpa.api.dao.DaoRegistry;
import ca.uhn.fhir.jpa.api.model.DaoMethodOutcome;
import ca.uhn.fhir.jpa.api.model.LazyDaoMethodOutcome;
import ca.uhn.fhir.jpa.model.cross.IBasePersistedResource;
import ca.uhn.fhir.jpa.model.entity.ResourceTable;
import ca.uhn.fhir.jpa.model.entity.StorageSettings;
import ca.uhn.fhir.model.api.StorageResponseCodeEnum;
import ca.uhn.fhir.rest.api.RestOperationTypeEnum;
import ca.uhn.fhir.rest.api.server.IPreResourceAccessDetails;
import ca.uhn.fhir.rest.api.server.IPreResourceShowDetails;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.api.server.SimplePreResourceAccessDetails;
import ca.uhn.fhir.rest.api.server.SimplePreResourceShowDetails;
import ca.uhn.fhir.rest.api.server.storage.IResourcePersistentId;
import ca.uhn.fhir.rest.api.server.storage.TransactionDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceGoneException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import ca.uhn.fhir.util.BundleUtil;
import ca.uhn.fhir.util.FhirTerser;
import ca.uhn.fhir.util.OperationOutcomeUtil;
import ca.uhn.fhir.util.ResourceReferenceInfo;
import ca.uhn.fhir.util.StopWatch;
import ca.uhn.fhir.util.UrlUtil;
import jakarta.inject.Inject;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
import org.hl7.fhir.instance.model.api.IBaseReference;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.InstantType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.commons.lang3.StringUtils.defaultString;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public abstract class BaseStorageDao {
	private static final Logger ourLog = LoggerFactory.getLogger(BaseStorageDao.class);

	public static final String OO_SEVERITY_ERROR = "error";
	public static final String OO_SEVERITY_INFO = "information";
	public static final String OO_SEVERITY_WARN = "warning";
	private static final String PROCESSING_SUB_REQUEST = "BaseStorageDao.processingSubRequest";

	protected static final String MESSAGE_KEY_DELETE_RESOURCE_NOT_EXISTING = "deleteResourceNotExisting";
	protected static final String MESSAGE_KEY_DELETE_RESOURCE_ALREADY_DELETED = "deleteResourceAlreadyDeleted";

	@Inject
	protected FhirContext myFhirContext;

	@Inject
	protected DaoRegistry myDaoRegistry;

	@Inject
	protected JpaStorageSettings myStorageSettings;

	/**
	 * May be overridden by subclasses to validate resources prior to storage
	 *
	 * @param theResource The resource that is about to be stored
	 * @deprecated Use {@link #preProcessResourceForStorage(IBaseResource, RequestDetails, TransactionDetails, boolean)} instead
	 */
	protected void preProcessResourceForStorage(IBaseResource theResource) {
		// nothing
		ourLog.debug("ahmet debugging: preProcessResourceForStorage");
	}

	/**
	 * May be overridden by subclasses to validate resources prior to storage
	 *
	 * @param theResource The resource that is about to be stored
	 * @since 5.3.0
	 */
	protected void preProcessResourceForStorage(
			IBaseResource theResource,
			RequestDetails theRequestDetails,
			TransactionDetails theTransactionDetails,
			boolean thePerformIndexing) {

		verifyResourceTypeIsAppropriateForDao(theResource);

		verifyResourceIdIsValid(theResource);

		verifyBundleTypeIsAppropriateForStorage(theResource);

		if (!getStorageSettings().getTreatBaseUrlsAsLocal().isEmpty()) {
			replaceAbsoluteReferencesWithRelative(theResource, myFhirContext.newTerser());
		}

		performAutoVersioning(theResource, thePerformIndexing);
	}

	/**
	 * Sanity check - Is this resource the right type for this DAO?
	 */
	private void verifyResourceTypeIsAppropriateForDao(IBaseResource theResource) {
		String type = getContext().getResourceType(theResource);
		if (getResourceName() != null && !getResourceName().equals(type)) {
			throw new InvalidRequestException(Msg.code(520)
					+ getContext()
							.getLocalizer()
							.getMessageSanitized(
									BaseStorageDao.class, "incorrectResourceType", type, getResourceName()));
		}
	}

	/**
	 * Verify that the resource ID is actually valid according to FHIR's rules
	 */
	private void verifyResourceIdIsValid(IBaseResource theResource) {
		if (theResource.getIdElement().hasIdPart()) {
			if (!theResource.getIdElement().isIdPartValid()) {
				throw new InvalidRequestException(Msg.code(521)
						+ getContext()
								.getLocalizer()
								.getMessageSanitized(
										BaseStorageDao.class,
										"failedToCreateWithInvalidId",
										theResource.getIdElement().getIdPart()));
			}
		}
	}

	/**
	 * Verify that we're not storing a Bundle with a disallowed bundle type
	 */
	private void verifyBundleTypeIsAppropriateForStorage(IBaseResource theResource) {
		if (theResource instanceof IBaseBundle) {
			Set<String> allowedBundleTypes = getStorageSettings().getBundleTypesAllowedForStorage();
			String bundleType = BundleUtil.getBundleType(getContext(), (IBaseBundle) theResource);
			bundleType = defaultString(bundleType);
			if (!allowedBundleTypes.contains(bundleType)) {
				String message = myFhirContext
						.getLocalizer()
						.getMessage(
								BaseStorageDao.class,
								"invalidBundleTypeForStorage",
								(isNotBlank(bundleType) ? bundleType : "(missing)"));
				throw new UnprocessableEntityException(Msg.code(522) + message);
			}
		}
	}

	/**
	 * Replace absolute references with relative ones if configured to do so
	 */
	private void replaceAbsoluteReferencesWithRelative(IBaseResource theResource, FhirTerser theTerser) {
		List<ResourceReferenceInfo> refs = theTerser.getAllResourceReferences(theResource);
		for (ResourceReferenceInfo nextRef : refs) {
			IIdType refId = nextRef.getResourceReference().getReferenceElement();
			if (refId != null && refId.hasBaseUrl()) {
				if (getStorageSettings().getTreatBaseUrlsAsLocal().contains(refId.getBaseUrl())) {
					IIdType newRefId = refId.toUnqualified();
					nextRef.getResourceReference().setReference(newRefId.getValue());
				}
			}
		}
	}

	/**
	 * Handle {@link JpaStorageSettings#getAutoVersionReferenceAtPaths() auto-populate-versions}
	 * <p>
	 * We only do this if thePerformIndexing is true because if it's false, that means
	 * we're in a FHIR transaction during the first phase of write operation processing,
	 * meaning that the versions of other resources may not have need updatd yet. For example
	 * we're about to store an Observation with a reference to a Patient, and that Patient
	 * is also being updated in the same transaction, during the first "no index" phase,
	 * the Patient will not yet have its version number incremented, so it would be wrong
	 * to use that value. During the second phase it is correct.
	 * <p>
	 */
	private void performAutoVersioning(IBaseResource theResource, boolean thePerformIndexing) {
		// ian: nop, we don't deal with indexing
	}

	protected DaoMethodOutcome toMethodOutcome(
			RequestDetails theRequest,
			@Nonnull final IBasePersistedResource theEntity,
			@Nonnull IBaseResource theResource,
			@Nullable String theMatchUrl,
			@Nonnull RestOperationTypeEnum theOperationType) {
		DaoMethodOutcome outcome = new DaoMethodOutcome();

		IResourcePersistentId persistentId = theEntity.getPersistentId();
		persistentId.setAssociatedResourceId(theResource.getIdElement());

		outcome.setPersistentId(persistentId);
		outcome.setMatchUrl(theMatchUrl);
		outcome.setOperationType(theOperationType);

		if (theEntity instanceof ResourceTable) {
			if (((ResourceTable) theEntity).isUnchangedInCurrentOperation()) {
				outcome.setNop(true);
			}
		}

		IIdType id = null;
		if (theResource.getIdElement().getValue() != null) {
			id = theResource.getIdElement();
		}
		if (id == null) {
			id = theEntity.getIdDt();
			if (getContext().getVersion().getVersion().isRi()) {
				id = getContext().getVersion().newIdType().setValue(id.getValue());
			}
		}

		outcome.setId(id);
		if (theEntity.getDeleted() == null) {
			outcome.setResource(theResource);
		}
		outcome.setEntity(theEntity);

		// Interceptor broadcast: STORAGE_PREACCESS_RESOURCES
		if (outcome.getResource() != null) {
			SimplePreResourceAccessDetails accessDetails = new SimplePreResourceAccessDetails(outcome.getResource());
			HookParams params = new HookParams()
					.add(IPreResourceAccessDetails.class, accessDetails)
					.add(RequestDetails.class, theRequest)
					.addIfMatchesType(ServletRequestDetails.class, theRequest);
			if (accessDetails.isDontReturnResourceAtIndex(0)) {
				outcome.setResource(null);
			}
		}

		// Interceptor broadcast: STORAGE_PRESHOW_RESOURCES
		// Note that this will only fire if someone actually goes to use the
		// resource in a response (it's their responsibility to call
		// outcome.fireResourceViewCallback())
		outcome.registerResourceViewCallback(() -> {
			if (outcome.getResource() != null) {
				SimplePreResourceShowDetails showDetails = new SimplePreResourceShowDetails(outcome.getResource());
				HookParams params = new HookParams()
						.add(IPreResourceShowDetails.class, showDetails)
						.add(RequestDetails.class, theRequest)
						.addIfMatchesType(ServletRequestDetails.class, theRequest);
				outcome.setResource(showDetails.getResource(0));
			}
		});

		return outcome;
	}

	protected DaoMethodOutcome toMethodOutcomeLazy(
			RequestDetails theRequest,
			IResourcePersistentId theResourcePersistentId,
			@Nonnull final Supplier<LazyDaoMethodOutcome.EntityAndResource> theEntity,
			Supplier<IIdType> theIdSupplier) {
		LazyDaoMethodOutcome outcome = new LazyDaoMethodOutcome(theResourcePersistentId);

		outcome.setEntitySupplier(theEntity);
		outcome.setIdSupplier(theIdSupplier);
		outcome.setEntitySupplierUseCallback(() -> {
			// Interceptor broadcast: STORAGE_PREACCESS_RESOURCES
			if (outcome.getResource() != null) {
				SimplePreResourceAccessDetails accessDetails =
						new SimplePreResourceAccessDetails(outcome.getResource());
				HookParams params = new HookParams()
						.add(IPreResourceAccessDetails.class, accessDetails)
						.add(RequestDetails.class, theRequest)
						.addIfMatchesType(ServletRequestDetails.class, theRequest);
				if (accessDetails.isDontReturnResourceAtIndex(0)) {
					outcome.setResource(null);
				}
			}

			// Interceptor broadcast: STORAGE_PRESHOW_RESOURCES
			// Note that this will only fire if someone actually goes to use the
			// resource in a response (it's their responsibility to call
			// outcome.fireResourceViewCallback())
			outcome.registerResourceViewCallback(() -> {
				if (outcome.getResource() != null) {
					SimplePreResourceShowDetails showDetails = new SimplePreResourceShowDetails(outcome.getResource());
					HookParams params = new HookParams()
							.add(IPreResourceShowDetails.class, showDetails)
							.add(RequestDetails.class, theRequest)
							.addIfMatchesType(ServletRequestDetails.class, theRequest);
					outcome.setResource(showDetails.getResource(0));
				}
			});
		});

		return outcome;
	}

	protected void doCallHooks(
			TransactionDetails theTransactionDetails,
			RequestDetails theRequestDetails,
			Pointcut thePointcut,
			HookParams theParams) {
		// ian: nothing
	}

	public IBaseOperationOutcome createErrorOperationOutcome(String theMessage, String theCode) {
		return createOperationOutcome(OO_SEVERITY_ERROR, theMessage, theCode);
	}

	public IBaseOperationOutcome createInfoOperationOutcome(String theMessage) {
		return createInfoOperationOutcome(theMessage, null);
	}

	public IBaseOperationOutcome createInfoOperationOutcome(
			String theMessage, @Nullable StorageResponseCodeEnum theStorageResponseCode) {
		return createOperationOutcome(OO_SEVERITY_INFO, theMessage, "informational", theStorageResponseCode);
	}

	private IBaseOperationOutcome createOperationOutcome(String theSeverity, String theMessage, String theCode) {
		return createOperationOutcome(theSeverity, theMessage, theCode, null);
	}

	protected IBaseOperationOutcome createOperationOutcome(
			String theSeverity,
			String theMessage,
			String theCode,
			@Nullable StorageResponseCodeEnum theStorageResponseCode) {
		IBaseOperationOutcome oo = OperationOutcomeUtil.newInstance(getContext());
		String detailSystem = null;
		String detailCode = null;
		String detailDescription = null;
		if (theStorageResponseCode != null) {
			detailSystem = theStorageResponseCode.getSystem();
			detailCode = theStorageResponseCode.getCode();
			detailDescription = theStorageResponseCode.getDisplay();
		}
		OperationOutcomeUtil.addIssue(
				getContext(), oo, theSeverity, theMessage, null, theCode, detailSystem, detailCode, detailDescription);
		return oo;
	}

	/**
	 * Creates a base method outcome for a delete request for the provided ID.
	 * <p>
	 * Additional information may be set on the outcome.
	 *
	 * @param theResourceId - the id of the object being deleted. Eg: Patient/123
	 */
	protected DaoMethodOutcome createMethodOutcomeForResourceId(
			String theResourceId, String theMessageKey, StorageResponseCodeEnum theStorageResponseCode) {
		DaoMethodOutcome outcome = new DaoMethodOutcome();

		IIdType id = getContext().getVersion().newIdType();
		id.setValue(theResourceId);
		outcome.setId(id);

		String message = getContext().getLocalizer().getMessage(BaseStorageDao.class, theMessageKey, id);
		String severity = "information";
		String code = "informational";
		IBaseOperationOutcome oo = createOperationOutcome(severity, message, code, theStorageResponseCode);
		outcome.setOperationOutcome(oo);

		return outcome;
	}

	@Nonnull
	protected ResourceGoneException createResourceGoneException(IBasePersistedResource theResourceEntity) {
		StringBuilder b = new StringBuilder();
		b.append("Resource was deleted at ");
		b.append(new InstantType(theResourceEntity.getDeleted()).getValueAsString());
		ResourceGoneException retVal = new ResourceGoneException(b.toString());
		retVal.setResourceId(theResourceEntity.getIdDt());
		return retVal;
	}

	/**
	 * Provide the JpaStorageSettings
	 */
	protected abstract JpaStorageSettings getStorageSettings();

	/**
	 * Returns the resource type for this DAO, or null if this is a system-level DAO
	 */
	@Nullable
	protected abstract String getResourceName();

	/**
	 * Provides the FHIR context
	 */
	protected abstract FhirContext getContext();

	protected void populateOperationOutcomeForUpdate(
			@Nullable StopWatch theItemStopwatch,
			DaoMethodOutcome theMethodOutcome,
			String theMatchUrl,
			RestOperationTypeEnum theOperationType) {
		String msg;
		StorageResponseCodeEnum outcome;

		if (theOperationType == RestOperationTypeEnum.PATCH) {

			if (theMatchUrl != null) {
				if (theMethodOutcome.isNop()) {
					outcome = StorageResponseCodeEnum.SUCCESSFUL_CONDITIONAL_PATCH_NO_CHANGE;
					msg = getContext()
							.getLocalizer()
							.getMessageSanitized(
									BaseStorageDao.class,
									"successfulPatchConditionalNoChange",
									theMethodOutcome.getId(),
									UrlUtil.sanitizeUrlPart(theMatchUrl),
									theMethodOutcome.getId());
				} else {
					outcome = StorageResponseCodeEnum.SUCCESSFUL_CONDITIONAL_PATCH;
					msg = getContext()
							.getLocalizer()
							.getMessageSanitized(
									BaseStorageDao.class,
									"successfulPatchConditional",
									theMethodOutcome.getId(),
									UrlUtil.sanitizeUrlPart(theMatchUrl),
									theMethodOutcome.getId());
				}
			} else {
				if (theMethodOutcome.isNop()) {
					outcome = StorageResponseCodeEnum.SUCCESSFUL_PATCH_NO_CHANGE;
					msg = getContext()
							.getLocalizer()
							.getMessageSanitized(
									BaseStorageDao.class, "successfulPatchNoChange", theMethodOutcome.getId());
				} else {
					outcome = StorageResponseCodeEnum.SUCCESSFUL_PATCH;
					msg = getContext()
							.getLocalizer()
							.getMessageSanitized(BaseStorageDao.class, "successfulPatch", theMethodOutcome.getId());
				}
			}

		} else if (theOperationType == RestOperationTypeEnum.CREATE) {

			if (theMatchUrl == null) {
				outcome = StorageResponseCodeEnum.SUCCESSFUL_CREATE;
				msg = getContext()
						.getLocalizer()
						.getMessageSanitized(BaseStorageDao.class, "successfulCreate", theMethodOutcome.getId());
			} else if (theMethodOutcome.isNop()) {
				outcome = StorageResponseCodeEnum.SUCCESSFUL_CREATE_WITH_CONDITIONAL_MATCH;
				msg = getContext()
						.getLocalizer()
						.getMessageSanitized(
								BaseStorageDao.class,
								"successfulCreateConditionalWithMatch",
								theMethodOutcome.getId(),
								UrlUtil.sanitizeUrlPart(theMatchUrl));
			} else {
				outcome = StorageResponseCodeEnum.SUCCESSFUL_CREATE_NO_CONDITIONAL_MATCH;
				msg = getContext()
						.getLocalizer()
						.getMessageSanitized(
								BaseStorageDao.class,
								"successfulCreateConditionalNoMatch",
								theMethodOutcome.getId(),
								UrlUtil.sanitizeUrlPart(theMatchUrl));
			}

		} else if (theMethodOutcome.isNop()) {

			if (theMatchUrl != null) {
				outcome = StorageResponseCodeEnum.SUCCESSFUL_UPDATE_WITH_CONDITIONAL_MATCH_NO_CHANGE;
				msg = getContext()
						.getLocalizer()
						.getMessageSanitized(
								BaseStorageDao.class,
								"successfulUpdateConditionalNoChangeWithMatch",
								theMethodOutcome.getId(),
								theMatchUrl);
			} else {
				outcome = StorageResponseCodeEnum.SUCCESSFUL_UPDATE_NO_CHANGE;
				msg = getContext()
						.getLocalizer()
						.getMessageSanitized(
								BaseStorageDao.class, "successfulUpdateNoChange", theMethodOutcome.getId());
			}

		} else {

			if (theMatchUrl != null) {
				if (theMethodOutcome.getCreated() == Boolean.TRUE) {
					outcome = StorageResponseCodeEnum.SUCCESSFUL_UPDATE_NO_CONDITIONAL_MATCH;
					msg = getContext()
							.getLocalizer()
							.getMessageSanitized(
									BaseStorageDao.class,
									"successfulUpdateConditionalNoMatch",
									theMethodOutcome.getId());
				} else {
					outcome = StorageResponseCodeEnum.SUCCESSFUL_UPDATE_WITH_CONDITIONAL_MATCH;
					msg = getContext()
							.getLocalizer()
							.getMessageSanitized(
									BaseStorageDao.class,
									"successfulUpdateConditionalWithMatch",
									theMethodOutcome.getId(),
									theMatchUrl);
				}
			} else if (theMethodOutcome.getCreated() == Boolean.TRUE) {
				outcome = StorageResponseCodeEnum.SUCCESSFUL_UPDATE_AS_CREATE;
				msg = getContext()
						.getLocalizer()
						.getMessageSanitized(
								BaseStorageDao.class, "successfulUpdateAsCreate", theMethodOutcome.getId());
			} else {
				outcome = StorageResponseCodeEnum.SUCCESSFUL_UPDATE;
				msg = getContext()
						.getLocalizer()
						.getMessageSanitized(BaseStorageDao.class, "successfulUpdate", theMethodOutcome.getId());
			}
		}

		if (theItemStopwatch != null) {
			String msgSuffix = getContext()
					.getLocalizer()
					.getMessageSanitized(BaseStorageDao.class, "successfulTimingSuffix", theItemStopwatch.getMillis());
			msg = msg + " " + msgSuffix;
		}

		theMethodOutcome.setOperationOutcome(createInfoOperationOutcome(msg, outcome));
		ourLog.debug(msg);
	}

	/**
	 * @see StorageSettings#getAutoVersionReferenceAtPaths()
	 */
	@Nonnull
	public static Set<IBaseReference> extractReferencesToAutoVersion(
			FhirContext theFhirContext, StorageSettings theStorageSettings, IBaseResource theResource) {
		Map<IBaseReference, Object> references = Collections.emptyMap();
		if (!theStorageSettings.getAutoVersionReferenceAtPaths().isEmpty()) {
			String resourceName = theFhirContext.getResourceType(theResource);
			for (String nextPath : theStorageSettings.getAutoVersionReferenceAtPathsByResourceType(resourceName)) {
				List<IBaseReference> nextReferences =
						theFhirContext.newTerser().getValues(theResource, nextPath, IBaseReference.class);
				for (IBaseReference next : nextReferences) {
					if (next.getReferenceElement().hasVersionIdPart()) {
						continue;
					}
					if (references.isEmpty()) {
						references = new IdentityHashMap<>();
					}
					references.put(next, null);
				}
			}
		}
		return references.keySet();
	}

	public static void clearRequestAsProcessingSubRequest(RequestDetails theRequestDetails) {
		if (theRequestDetails != null) {
			theRequestDetails.getUserData().remove(PROCESSING_SUB_REQUEST);
		}
	}

	public static void markRequestAsProcessingSubRequest(RequestDetails theRequestDetails) {
		if (theRequestDetails != null) {
			theRequestDetails.getUserData().put(PROCESSING_SUB_REQUEST, Boolean.TRUE);
		}
	}
}
