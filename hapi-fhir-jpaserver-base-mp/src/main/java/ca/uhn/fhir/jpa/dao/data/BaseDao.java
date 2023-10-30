package ca.uhn.fhir.jpa.dao.data;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.PersistenceContextType;

import java.util.Optional;

public class BaseDao<T> implements IHapiFhirJpaRepository {

	@PersistenceContext(type = PersistenceContextType.TRANSACTION)
	protected EntityManager entityManager;

	final Class<T> typeParameterClass;

	public BaseDao(Class<T> typeParameterClass) {
		this.typeParameterClass = typeParameterClass;
	}

	public Optional<T> findById(Long id) {
		return Optional.ofNullable(entityManager.find(typeParameterClass, id));
	}

	public void save(T t) {
		entityManager.persist(t);
	}
}
