package ca.uhn.fhir.jpa.dao.r4;

import ca.uhn.fhir.jpa.dao.JpaResourceDao;
import jakarta.enterprise.context.RequestScoped;
import org.hl7.fhir.r4.model.Patient;

//// THIS CLASS SHOULD BE CODE GENERATED!
@RequestScoped
public class DaoPatientR4 extends JpaResourceDao<Patient> {
	public DaoPatientR4() {
		super();
		this.setResourceType(Patient.class);
	}
}
