/*
 * #%L
 * HAPI FHIR JPA Server
 * %%
 * Copyright (C) 2014 - 2023 Smile CDR, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package ca.uhn.fhir.jpa.dao.data;

import ca.uhn.fhir.jpa.dao.data.custom.IForcedIdQueries;
import ca.uhn.fhir.jpa.model.entity.ForcedId;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

import java.util.*;

public class IForcedIdDao extends BaseDao<ForcedId> implements IForcedIdQueries {

	@PersistenceContext
	private EntityManager entityManager;

	public IForcedIdDao() {
		super(ForcedId.class);
	}

	public List<ForcedId> findAllByResourcePid(List<Long> theResourcePids) {
		return entityManager
				.createQuery("SELECT f FROM ForcedId f WHERE f.myResourcePid IN (:resource_pids)", ForcedId.class)
				.setParameter("resource_pids", theResourcePids)
				.getResultList();
	}

	public Optional<ForcedId> findByResourcePid(Long theResourcePid) {
		return entityManager
				.createQuery("SELECT f FROM ForcedId f WHERE f.myResourcePid = :resource_pid", ForcedId.class)
				.setParameter("resource_pid", theResourcePid)
				.getResultList()
				.stream()
				.findFirst();
	}

	public void deleteByPid(Long theId) {
		entityManager
				.createQuery("DELETE FROM ForcedId t WHERE t.myId = :pid")
				.setParameter("pid", theId)
				.executeUpdate();
	}
	/**
	 * This method returns a Collection where each row is an element in the collection. Each element in the collection
	 * is an object array, where the order matters (the array represents columns returned by the query).
	 * Deleted resources are not filtered.
	 */
	public Collection<Object[]> findAndResolveByForcedIdWithNoTypeIncludeDeleted(
			String theResourceType, Collection<String> theForcedIds) {
		return findAndResolveByForcedIdWithNoType(theResourceType, theForcedIds, false);
	}

	/**
	 * This method returns a Collection where each row is an element in the collection. Each element in the collection
	 * is an object array, where the order matters (the array represents columns returned by the query).
	 * Deleted resources are optionally filtered. Be careful if you change this query in any way.
	 */
	public Collection<Object[]> findAndResolveByForcedIdWithNoType(
			String theResourceType, Collection<String> theForcedIds, boolean theExcludeDeleted) {
		String query = "" + "SELECT "
				+ "   f.myResourceType, f.myResourcePid, f.myForcedId, t.myDeleted "
				+ "FROM ForcedId f "
				+ "JOIN ResourceTable t ON t.myId = f.myResourcePid "
				+ "WHERE f.myResourceType = :resource_type AND f.myForcedId IN ( :forced_id )";

		if (theExcludeDeleted) {
			query += " AND t.myDeleted IS NULL";
		}

		return entityManager
				.createQuery(query)
				.setParameter("resource_type", theResourceType)
				.setParameter("forced_id", theForcedIds)
				.getResultList();
	}

	/**
	 * This method returns a Collection where each row is an element in the collection. Each element in the collection
	 * is an object array, where the order matters (the array represents columns returned by the query).
	 * Deleted resources are optionally filtered. Be careful if you change this query in any way.
	 */
	public Collection<Object[]> findAndResolveByForcedIdWithNoTypeInPartition(
			String theResourceType,
			Collection<String> theForcedIds,
			Collection<Integer> thePartitionId,
			boolean theExcludeDeleted) {
		String query = "" + "SELECT "
				+ "   f.myResourceType, f.myResourcePid, f.myForcedId, t.myDeleted "
				+ "FROM ForcedId f "
				+ "JOIN ResourceTable t ON t.myId = f.myResourcePid "
				+ "WHERE f.myResourceType = :resource_type AND f.myForcedId IN ( :forced_id ) AND f.myPartitionIdValue IN ( :partition_id )";

		if (theExcludeDeleted) {
			query += " AND t.myDeleted IS NULL";
		}

		return entityManager
				.createQuery(query)
				.setParameter("resource_type", theResourceType)
				.setParameter("forced_id", theForcedIds)
				.setParameter("partition_id", thePartitionId)
				.getResultList();
	}

	/**
	 * This method returns a Collection where each row is an element in the collection. Each element in the collection
	 * is an object array, where the order matters (the array represents columns returned by the query).
	 * Deleted resources are optionally filtered. Be careful if you change this query in any way.
	 */
	public Collection<Object[]> findAndResolveByForcedIdWithNoTypeInPartitionNull(
			String theResourceType, Collection<String> theForcedIds, boolean theExcludeDeleted) {
		String query = "" + "SELECT "
				+ "   f.myResourceType, f.myResourcePid, f.myForcedId, t.myDeleted "
				+ "FROM ForcedId f "
				+ "JOIN ResourceTable t ON t.myId = f.myResourcePid "
				+ "WHERE f.myResourceType = :resource_type AND f.myForcedId IN ( :forced_id ) AND f.myPartitionIdValue IS NULL";

		if (theExcludeDeleted) {
			query += " AND t.myDeleted IS NULL";
		}

		return entityManager
				.createQuery(query)
				.setParameter("resource_type", theResourceType)
				.setParameter("forced_id", theForcedIds)
				.getResultList();
	}

	/**
	 * This method returns a Collection where each row is an element in the collection. Each element in the collection
	 * is an object array, where the order matters (the array represents columns returned by the query).
	 * Deleted resources are optionally filtered. Be careful if you change this query in any way.
	 */
	public Collection<Object[]> findAndResolveByForcedIdWithNoTypeInPartitionIdOrNullPartitionId(
			String theResourceType,
			Collection<String> theForcedIds,
			List<Integer> thePartitionIdsWithoutDefault,
			boolean theExcludeDeleted) {
		String query = "" + "SELECT "
				+ "   f.myResourceType, f.myResourcePid, f.myForcedId, t.myDeleted "
				+ "FROM ForcedId f "
				+ "JOIN ResourceTable t ON t.myId = f.myResourcePid "
				+ "WHERE f.myResourceType = :resource_type AND f.myForcedId IN ( :forced_id ) AND (f.myPartitionIdValue IS NULL OR f.myPartitionIdValue IN ( :partition_id ))";

		if (theExcludeDeleted) {
			query += " AND t.myDeleted IS NULL";
		}

		return entityManager
				.createQuery(query)
				.setParameter("resource_type", theResourceType)
				.setParameter("forced_id", theForcedIds)
				.setParameter("partition_id", thePartitionIdsWithoutDefault)
				.getResultList();
	}
}
