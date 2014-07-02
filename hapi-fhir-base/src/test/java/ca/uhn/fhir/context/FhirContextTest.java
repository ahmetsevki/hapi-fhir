package ca.uhn.fhir.context;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import ca.uhn.fhir.model.dstu.resource.Patient;
import ca.uhn.fhir.model.dstu.resource.ValueSet;

public class FhirContextTest {

	@Test
	public void testIncrementalScan() {
		
		FhirContext ctx = new FhirContext();
		ctx.getResourceDefinition(ValueSet.class);
		ctx.getResourceDefinition(Patient.class);
	}
	
}
