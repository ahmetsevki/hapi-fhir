package ca.uhn.fhir.jpa.dao.data;

import jakarta.persistence.EntityManager;

import java.util.Optional;

public class BaseDao<T> implements IHapiFhirJpaRepository {

	protected EntityManager entityManager;

	final Class<T> typeParameterClass;

	public BaseDao(Class<T> typeParameterClass, EntityManager entityManager) {
		this.typeParameterClass = typeParameterClass;
		this.entityManager = entityManager;
	}

	public Optional<T> findById(Long id) {
		return Optional.ofNullable(entityManager.find(typeParameterClass, id));
	}

	public void save(T t) {
		entityManager.persist(t);
	}
}
