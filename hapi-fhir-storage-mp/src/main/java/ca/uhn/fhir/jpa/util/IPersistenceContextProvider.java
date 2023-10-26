package ca.uhn.fhir.jpa.util;

import jakarta.persistence.EntityManager;

public interface IPersistenceContextProvider {
	public EntityManager getEntityManager();
}
