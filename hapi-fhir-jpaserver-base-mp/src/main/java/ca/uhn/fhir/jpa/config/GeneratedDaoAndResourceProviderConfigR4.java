package ca.uhn.fhir.jpa.config;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.rest.api.IResourceSupportedSvc;
import ca.uhn.fhir.rest.server.provider.ResourceProviderFactory;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import java.util.ArrayList;
import java.util.List;

// THIS CLASS SHOULD BE CODE GENERATED!
public class GeneratedDaoAndResourceProviderConfigR4 {
	@Inject
	FhirContext myFhirContext;

	@Named("myResourceProvidersR4")
	public ResourceProviderFactory resourceProvidersR4(IResourceSupportedSvc theResourceSupportedSvc) {
		ResourceProviderFactory retVal = new ResourceProviderFactory();
		retVal.addSupplier(() -> theResourceSupportedSvc.isSupported("Patient") ? rpPatientR4() : null);
		return retVal;
	}

	@Named("myResourceDaosR4")
	public List<IFhirResourceDao<?>> resourceDaosR4(IResourceSupportedSvc theResourceSupportedSvc) {
		List<IFhirResourceDao<?>> retVal = new ArrayList<IFhirResourceDao<?>>();
		if (theResourceSupportedSvc.isSupported("Patient")) {
			retVal.add(daoPatientR4());
		}
		return retVal;
	}

	@Named("myPatientDaoR4")
	public IFhirResourceDao<org.hl7.fhir.r4.model.Patient> daoPatientR4() {

		ca.uhn.fhir.jpa.dao.JpaResourceDao<org.hl7.fhir.r4.model.Patient> retVal;
		retVal = new ca.uhn.fhir.jpa.dao.JpaResourceDao<>();
		retVal.setResourceType(org.hl7.fhir.r4.model.Patient.class);
		retVal.setContext(myFhirContext);
		return retVal;
	}

	@Named("myPatientRpR4")
	public ca.uhn.fhir.jpa.rp.r4.PatientResourceProvider rpPatientR4() {
		ca.uhn.fhir.jpa.rp.r4.PatientResourceProvider retVal;
		retVal = new ca.uhn.fhir.jpa.rp.r4.PatientResourceProvider(myFhirContext);
		retVal.setDao(daoPatientR4());
		return retVal;
	}
}
