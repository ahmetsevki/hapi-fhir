package ca.uhn.fhir.jpa.config;

import ca.uhn.fhir.jpa.dao.JpaResourceDao;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Named;
import org.hl7.fhir.r4.model.Patient;

// THIS CLASS SHOULD BE CODE GENERATED!
@RequestScoped
public class GeneratedDaoAndResourceProviderConfigR4 {

	@Named("myPatientDaoR4")
	@RequestScoped
	public static class DaoPatientR4 extends JpaResourceDao<Patient> {
		public DaoPatientR4() {
			super();
			this.setResourceType(Patient.class);
		}
	}
}
