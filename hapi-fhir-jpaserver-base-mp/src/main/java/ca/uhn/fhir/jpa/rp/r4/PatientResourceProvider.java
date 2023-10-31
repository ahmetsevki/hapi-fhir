package ca.uhn.fhir.jpa.rp.r4;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.dao.JpaResourceDao;
import ca.uhn.fhir.jpa.provider.BaseJpaResourceProvider;
import ca.uhn.fhir.rest.api.Constants;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import org.hl7.fhir.r4.model.Patient;

// THIS CLASS SHOULD BE CODE GENERATED!

@Path("/Patient")
@RequestScoped
@jakarta.ws.rs.Produces({MediaType.APPLICATION_JSON, Constants.CT_FHIR_JSON, Constants.CT_FHIR_XML})
public class PatientResourceProvider extends BaseJpaResourceProvider<Patient> {

	@Override
	public Class<Patient> getResourceType() {
		return Patient.class;
	}

	@Inject
	public PatientResourceProvider(
			@Named("fhirContextR4") FhirContext ctx, @Named("myPatientDaoR4") JpaResourceDao<Patient> dao) {
		super(ctx);
		this.setDao(dao);
	}
	// search will be different for every resource, hence it we need a codegen
	//	@Search
	//	public ca.uhn.fhir.rest.api.server.IBundleProvider search(....)
}
