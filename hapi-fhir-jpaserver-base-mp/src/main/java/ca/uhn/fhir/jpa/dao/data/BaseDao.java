package ca.uhn.fhir.jpa.dao.data;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.PersistenceContext;

import java.util.Optional;
import java.util.function.Consumer;

public class BaseDao<T> implements IHapiFhirJpaRepository {
	@PersistenceContext
	protected EntityManager entityManager;

	final Class<T> typeParameterClass;

	public BaseDao(Class<T> typeParameterClass) {
		this.typeParameterClass = typeParameterClass;
	}

	public Optional<T> findById(Long id) {
		return Optional.ofNullable(entityManager.find(typeParameterClass, id));
	}

	public void save(T t) {
		executeInsideTransaction(entityManager -> entityManager.persist(t));
	}

	private void executeInsideTransaction(Consumer<EntityManager> action) {
		EntityTransaction tx = entityManager.getTransaction();
		try {
			tx.begin();
			action.accept(entityManager);
			tx.commit();
		} catch (RuntimeException e) {
			tx.rollback();
			throw e;
		}
	}
}
