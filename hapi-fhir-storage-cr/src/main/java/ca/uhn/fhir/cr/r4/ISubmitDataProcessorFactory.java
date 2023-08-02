package ca.uhn.fhir.cr.r4;

import org.opencds.cqf.cql.evaluator.measure.r4.R4SubmitDataService;
import org.opencds.cqf.fhir.api.Repository;

public interface ISubmitDataProcessorFactory {
	R4SubmitDataService create(Repository theRepository);
}
