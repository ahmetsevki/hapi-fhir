package ca.uhn.fhir.jpa.bulk.imprt2;

import ca.uhn.fhir.batch2.api.IJobDataSink;
import ca.uhn.fhir.batch2.api.IJobStepWorker;
import ca.uhn.fhir.batch2.api.JobExecutionFailedException;
import ca.uhn.fhir.batch2.api.StepExecutionDetails;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.interceptor.model.RequestPartitionId;
import ca.uhn.fhir.jpa.api.dao.DaoRegistry;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.api.dao.IFhirSystemDao;
import ca.uhn.fhir.jpa.dao.index.IdHelperService;
import ca.uhn.fhir.jpa.dao.tx.HapiTransactionService;
import ca.uhn.fhir.jpa.model.config.PartitionSettings;
import ca.uhn.fhir.jpa.partition.SystemRequestDetails;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.api.server.storage.ResourcePersistentId;
import ca.uhn.fhir.rest.api.server.storage.TransactionDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.PreconditionFailedException;
import org.apache.commons.io.LineIterator;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class ConsumeFilesStep implements IJobStepWorker {

	private static final Logger ourLog = LoggerFactory.getLogger(ConsumeFilesStep.class);
	@Autowired
	private FhirContext myCtx;
	@Autowired
	private DaoRegistry myDaoRegistry;
	@Autowired
	private HapiTransactionService myHapiTransactionService;
	@Autowired
	private IdHelperService myIdHelperService;
	@Autowired
	private PartitionSettings myPartitionSettings;
	@Autowired
	private IFhirSystemDao<?, ?> mySystemDao;

	@Override
	public RunOutcome run(StepExecutionDetails theStepExecutionDetails, IJobDataSink theDataSink) {

		String ndjson = (String) theStepExecutionDetails.getData().get(FetchFilesStep.KEY_NDJSON);
		String sourceName = (String) theStepExecutionDetails.getData().get(FetchFilesStep.KEY_SOURCE_NAME);

		IParser jsonParser = myCtx.newJsonParser();
		LineIterator lineIter = new LineIterator(new StringReader(ndjson));
		List<IBaseResource> resources = new ArrayList<>();
		while (lineIter.hasNext()) {
			String next = lineIter.next();
			if (isNotBlank(next)) {
				IBaseResource parsed;
				try {
					parsed = jsonParser.parseResource(next);
				} catch (DataFormatException e) {
					throw new JobExecutionFailedException("Failed to parse resource: " + e, e);
				}
				resources.add(parsed);
			}
		}

		ourLog.info("Bulk loading {} resources from source {}", resources.size(), sourceName);

		storeResources(resources);

		return new RunOutcome(resources.size());
	}

	public void storeResources(List<IBaseResource> resources) {
		RequestDetails requestDetails = new SystemRequestDetails();
		TransactionDetails transactionDetails = new TransactionDetails();
		myHapiTransactionService.execute(requestDetails, transactionDetails, tx -> storeResourcesInsideTransaction(resources, requestDetails, transactionDetails));
	}

	private Void storeResourcesInsideTransaction(List<IBaseResource> theResources, RequestDetails theRequestDetails, TransactionDetails theTransactionDetails) {
		Map<IIdType, IBaseResource> ids = new HashMap<>();
		for (IBaseResource next : theResources) {
			if (!next.getIdElement().hasIdPart()) {
				continue;
			}

			IIdType id = next.getIdElement();
			if (!id.hasResourceType()) {
				id.setParts(null, myCtx.getResourceType(next), id.getIdPart(), id.getVersionIdPart());
			}
			ids.put(id, next);
		}

		List<IIdType> idsList = new ArrayList<>(ids.keySet());
		List<ResourcePersistentId> resolvedIds = myIdHelperService.resolveResourcePersistentIdsWithCache(RequestPartitionId.allPartitions(), idsList, true);
		for (ResourcePersistentId next : resolvedIds) {
			IIdType resId = next.getAssociatedResourceId();
			theTransactionDetails.addResolvedResourceId(resId, next);
			ids.remove(resId);
		}
		for (IIdType next : ids.keySet()) {
			theTransactionDetails.addResolvedResourceId(next, null);
		}

		mySystemDao.preFetchResources(resolvedIds);

		for (IBaseResource next : theResources) {
			updateResource(theRequestDetails, theTransactionDetails, next);
		}

		return null;
	}

	private <T extends IBaseResource> void updateResource(RequestDetails theRequestDetails, TransactionDetails theTransactionDetails, T theResource) {
		IFhirResourceDao<T> dao = myDaoRegistry.getResourceDao(theResource);
		try {
			dao.update(theResource, null, true, false, theRequestDetails, theTransactionDetails);
		} catch (InvalidRequestException | PreconditionFailedException e) {
			String msg = "Failure during bulk import: " + e;
			ourLog.error(msg);
			throw new JobExecutionFailedException(msg, e);
		}
	}
}
