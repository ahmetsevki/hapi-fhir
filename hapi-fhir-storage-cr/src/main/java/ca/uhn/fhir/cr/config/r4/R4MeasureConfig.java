package ca.uhn.fhir.cr.config.r4;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.cr.common.IRepositoryFactory;
import ca.uhn.fhir.cr.config.ProviderLoader;
import ca.uhn.fhir.cr.config.ProviderSelector;
import ca.uhn.fhir.cr.config.RepositoryConfig;
import ca.uhn.fhir.cr.r4.ICareGapsServiceFactory;
import ca.uhn.fhir.cr.r4.IMeasureServiceFactory;
import ca.uhn.fhir.cr.r4.ISubmitDataProcessorFactory;
import ca.uhn.fhir.cr.r4.measure.CareGapsOperationProvider;
import ca.uhn.fhir.cr.r4.measure.MeasureOperationsProvider;
import ca.uhn.fhir.cr.r4.measure.MeasureService;
import ca.uhn.fhir.cr.r4.measure.SubmitDataProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import org.opencds.cqf.cql.evaluator.measure.CareGapsProperties;
import org.opencds.cqf.cql.evaluator.measure.MeasureEvaluationOptions;
import org.opencds.cqf.cql.evaluator.measure.r4.R4CareGapsService;
import org.opencds.cqf.cql.evaluator.measure.r4.R4SubmitDataService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executor;

@Configuration
@Import({RepositoryConfig.class})
public class R4MeasureConfig {
	@Bean
	IMeasureServiceFactory r4MeasureServiceFactory(
			IRepositoryFactory theRepositoryFactory, MeasureEvaluationOptions theEvaluationOptions) {
		return rd -> new MeasureService(theRepositoryFactory.create(rd), theEvaluationOptions);
	}

	@Bean
	ICareGapsServiceFactory careGapsServiceFactory(
			IRepositoryFactory theRepositoryFactory,
			CareGapsProperties theCareGapsProperties,
			MeasureEvaluationOptions theMeasureEvaluationOptions,
			@Qualifier("cqlExecutor") Executor theExecutor) {
		return rd -> new R4CareGapsService(
				theCareGapsProperties,
				theRepositoryFactory.create(rd),
				theMeasureEvaluationOptions,
				theExecutor,
				rd.getFhirServerBase());
	}

	@Bean
	CareGapsOperationProvider r4CareGapsOperationProvider() {
		return new CareGapsOperationProvider();
	}

	@Bean
	ISubmitDataProcessorFactory r4SubmitDataProcessorFactory() {
		return r -> new R4SubmitDataService(r);
	}

	@Bean
	SubmitDataProvider r4SubmitDataProvider() {
		return new SubmitDataProvider();
	}

	@Bean
	MeasureOperationsProvider r4MeasureOperationsProvider() {
		return new MeasureOperationsProvider();
	}

	@Bean
	public ProviderLoader r4PdLoader(
			ApplicationContext theApplicationContext, FhirContext theFhirContext, RestfulServer theRestfulServer) {

		var selector = new ProviderSelector(
				theFhirContext,
				Map.of(
						FhirVersionEnum.R4,
						Arrays.asList(
								MeasureOperationsProvider.class,
								SubmitDataProvider.class,
								CareGapsOperationProvider.class)));

		return new ProviderLoader(theRestfulServer, theApplicationContext, selector);
	}
}
