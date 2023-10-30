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

import ca.uhn.fhir.jpa.model.entity.ResourceTable;
import jakarta.enterprise.context.RequestScoped;

import java.util.*;

@RequestScoped
public class IResourceTableDao extends BaseDao<ResourceTable> {

	public IResourceTableDao() {
		super(ResourceTable.class);
	}

	public List<Long> findIdsOfDeletedResources() {
		return entityManager
				.createQuery("SELECT t.myId FROM ResourceTable t WHERE t.myDeleted IS NOT NULL")
				.getResultList();
	}

	public List<Long> findIdsOfDeletedResourcesOfType(String theResourceName) {
		return entityManager
				.createQuery(
						"SELECT t.myId FROM ResourceTable t WHERE t.myResourceType = :restype AND t.myDeleted IS NOT NULL")
				.setParameter("restype", theResourceName)
				.getResultList();
	}

	public List<Long> findIdsOfDeletedResourcesOfType(Long theResourceId, String theResourceName) {
		return entityManager
				.createQuery(
						"SELECT t.myId FROM ResourceTable t WHERE t.myId = :resid AND t.myResourceType = :restype AND t.myDeleted IS NOT NULL")
				.setParameter("restype", theResourceName)
				.setParameter("resid", theResourceId)
				.getResultList();
	}

	public List<Map<?, ?>> getResourceCounts() {
		return entityManager
				.createQuery(
						"SELECT t.myResourceType as type, COUNT(t.myResourceType) as count FROM ResourceTable t GROUP BY t.myResourceType")
				.getResultList();
	}

	public List<Long> findIdsOfResourcesWithinUpdatedRangeOrderedFromNewest(Date theLow, Date theHigh) {
		return entityManager
				.createQuery(
						"SELECT t.myId FROM ResourceTable t WHERE t.myUpdated >= :low AND t.myUpdated <= :high ORDER BY t.myUpdated DESC")
				.setParameter("low", theLow)
				.setParameter("high", theHigh)
				.getResultList();
	}

	public List<Long> findIdsOfResourcesWithinUpdatedRangeOrderedFromOldest(Date theLow, Date theHigh) {
		return entityManager
				.createQuery(
						"SELECT t.myId FROM ResourceTable t WHERE t.myUpdated >= :low AND t.myUpdated <= :high ORDER BY t.myUpdated ASC")
				.setParameter("low", theLow)
				.setParameter("high", theHigh)
				.getResultList();
	}

	/**
	 * @return List of arrays containing [PID, resourceType, lastUpdated]
	 */
	public List<Object[]> findIdsTypesAndUpdateTimesOfResourcesWithinUpdatedRangeOrderedFromOldest(
			Date theLow, Date theHigh) {
		return entityManager
				.createQuery(
						"SELECT t.myId, t.myResourceType, t.myUpdated FROM ResourceTable t WHERE t.myUpdated >= :low AND t.myUpdated <= :high ORDER BY t.myUpdated ASC")
				.setParameter("low", theLow)
				.setParameter("high", theHigh)
				.getResultList();
	}

	/**
	 * @return List of arrays containing [PID, resourceType, lastUpdated]
	 */
	public List<Object[]> findIdsTypesAndUpdateTimesOfResourcesWithinUpdatedRangeOrderedFromOldestForPartitionIds(
			Date theLow, Date theHigh, List<Integer> theRequestPartitionIds) {
		return entityManager
				.createQuery(
						"SELECT t.myId, t.myResourceType, t.myUpdated FROM ResourceTable t WHERE t.myUpdated >= :low AND t.myUpdated <= :high AND t.myPartitionIdValue IN (:partition_ids) ORDER BY t.myUpdated ASC")
				.setParameter("low", theLow)
				.setParameter("high", theHigh)
				.setParameter("partition_ids", theRequestPartitionIds)
				.getResultList();
	}

	/**
	 * @return List of arrays containing [PID, resourceType, lastUpdated]
	 */
	public List<Object[]> findIdsTypesAndUpdateTimesOfResourcesWithinUpdatedRangeOrderedFromOldestForDefaultPartition(
			Date theLow, Date theHigh) {
		return entityManager
				.createQuery(
						"SELECT t.myId, t.myResourceType, t.myUpdated FROM ResourceTable t WHERE t.myUpdated >= :low AND t.myUpdated <= :high ORDER BY t.myUpdated ASC")
				.setParameter("low", theLow)
				.setParameter("high", theHigh)
				.getResultList();
	}

	// TODO in the future, consider sorting by pid as well so batch jobs process in the same order across restarts
	public List<Long> findIdsOfPartitionedResourcesWithinUpdatedRangeOrderedFromOldest(
			Date theLow, Date theHigh, Integer theRequestPartitionId) {
		return entityManager
				.createQuery(
						"SELECT t.myId FROM ResourceTable t WHERE t.myUpdated >= :low AND t.myUpdated <= :high AND t.myPartitionIdValue = :partition_id ORDER BY t.myUpdated ASC")
				.setParameter("low", theLow)
				.setParameter("high", theHigh)
				.setParameter("partition_id", theRequestPartitionId)
				.getResultList();
	}

	public List<Long> findIdsOfResourcesWithinUpdatedRangeOrderedFromOldest(
			String theResourceType, Date theLow, Date theHigh) {
		return entityManager
				.createQuery(
						"SELECT t.myId FROM ResourceTable t WHERE t.myUpdated >= :low AND t.myUpdated <= :high AND t.myResourceType = :restype ORDER BY t.myUpdated ASC",
						Long.class)
				.setParameter("low", theLow)
				.setParameter("high", theHigh)
				.setParameter("restype", theResourceType)
				.getResultList();
	}

	public void updateLastUpdated(Long theId, Date theUpdated) {
		entityManager
				.createQuery("UPDATE ResourceTable t SET t.myUpdated = :updated WHERE t.myId = :id")
				.setParameter("id", theId)
				.setParameter("updated", theUpdated)
				.executeUpdate();
	}

	public void deleteByPid(Long theId) {
		entityManager
				.createQuery("DELETE FROM ResourceTable t WHERE t.myId = :pid")
				.setParameter("pid", theId)
				.executeUpdate();
	}

	/**
	 * This method returns a Collection where each row is an element in the collection. Each element in the collection
	 * is an object array, where the order matters (the array represents columns returned by the query). Be careful if you change this query in any way.
	 */
	public Collection<Object[]> findLookupFieldsByResourcePid(List<Long> thePids) {
		return entityManager
				.createQuery("SELECT t.myResourceType, t.myId, t.myDeleted FROM ResourceTable t WHERE t.myId IN (:pid)")
				.setParameter("pid", thePids)
				.getResultList();
	}

	/**
	 * This method returns a Collection where each row is an element in the collection. Each element in the collection
	 * is an object array, where the order matters (the array represents columns returned by the query). Be careful if you change this query in any way.
	 */
	public Collection<Object[]> findLookupFieldsByResourcePidInPartitionIds(
			List<Long> thePids, Collection<Integer> thePartitionId) {
		return entityManager
				.createQuery(
						"SELECT t.myResourceType, t.myId, t.myDeleted FROM ResourceTable t WHERE t.myId IN (:pid) AND t.myPartitionIdValue IN :partition_id")
				.setParameter("pid", thePids)
				.setParameter("partition_id", thePartitionId)
				.getResultList();
	}

	/**
	 * This method returns a Collection where each row is an element in the collection. Each element in the collection
	 * is an object array, where the order matters (the array represents columns returned by the query). Be careful if you change this query in any way.
	 */
	public Collection<Object[]> findLookupFieldsByResourcePidInPartitionIdsOrNullPartition(
			List<Long> thePids, Collection<Integer> thePartitionId) {
		return entityManager
				.createQuery(
						"SELECT t.myResourceType, t.myId, t.myDeleted FROM ResourceTable t WHERE t.myId IN (:pid) AND (t.myPartitionIdValue IS NULL OR t.myPartitionIdValue IN :partition_id)")
				.setParameter("pid", thePids)
				.setParameter("partition_id", thePartitionId)
				.getResultList();
	}

	/**
	 * This method returns a Collection where each row is an element in the collection. Each element in the collection
	 * is an object array, where the order matters (the array represents columns returned by the query). Be careful if you change this query in any way.
	 */
	public Collection<Object[]> findLookupFieldsByResourcePidInPartitionNull(List<Long> thePids) {
		return entityManager
				.createQuery(
						"SELECT t.myResourceType, t.myId, t.myDeleted FROM ResourceTable t WHERE t.myId IN (:pid) AND t.myPartitionIdValue IS NULL")
				.setParameter("pid", thePids)
				.getResultList();
	}

	public Long findCurrentVersionByPid(Long thePid) {
		return entityManager
				.createQuery("SELECT t.myVersion FROM ResourceTable t WHERE t.myId = :pid", Long.class)
				.setParameter("pid", thePid)
				.getSingleResult();
	}

	/**
	 * This query will return rows with the following values:
	 * Id (resource pid - long), ResourceType (Patient, etc), version (long)
	 * Order matters!
	 * @param pid - list of pids to get versions for
	 * @return
	 */
	public Collection<Object[]> getResourceVersionsForPid(List<Long> pid) {
		return entityManager
				.createQuery(
						"SELECT t.myId, t.myResourceType, t.myVersion FROM ResourceTable t WHERE t.myId IN ( :pid )")
				.setParameter("pid", pid)
				.getResultList();
	}

	public Optional<ResourceTable> readByPartitionIdNull(Long theResourceId) {
		return entityManager
				.createQuery(
						"SELECT t FROM ResourceTable t LEFT JOIN FETCH t.myForcedId WHERE t.myPartitionId.myPartitionId IS NULL AND t.myId = :pid",
						ResourceTable.class)
				.setParameter("pid", theResourceId)
				.setMaxResults(1)
				.getResultList()
				.stream()
				.findFirst();
	}

	public Optional<ResourceTable> readByPartitionId(int thePartitionId, Long theResourceId) {
		return entityManager
				.createQuery(
						"SELECT t FROM ResourceTable t LEFT JOIN FETCH t.myForcedId WHERE t.myPartitionId.myPartitionId = :partitionId AND t.myId = :pid",
						ResourceTable.class)
				.setParameter("pid", theResourceId)
				.setParameter("partitionId", thePartitionId)
				.setMaxResults(1)
				.getResultList()
				.stream()
				.findFirst();
	}

	public Optional<ResourceTable> readByPartitionIdsOrNull(Collection<Integer> thrValues, Long theResourceId) {
		return entityManager
				.createQuery(
						"SELECT t FROM ResourceTable t LEFT JOIN FETCH t.myForcedId WHERE (t.myPartitionId.myPartitionId IS NULL OR t.myPartitionId.myPartitionId IN (:partitionIds)) AND t.myId = :pid",
						ResourceTable.class)
				.setParameter("pid", theResourceId)
				.setParameter("partitionIds", thrValues)
				.setMaxResults(1)
				.getResultList()
				.stream()
				.findFirst();
	}

	public Optional<ResourceTable> readByPartitionIds(Collection<Integer> thrValues, Long theResourceId) {
		return entityManager
				.createQuery(
						"SELECT t FROM ResourceTable t LEFT JOIN FETCH t.myForcedId WHERE t.myPartitionId.myPartitionId IN (:partitionIds) AND t.myId = :pid",
						ResourceTable.class)
				.setParameter("pid", theResourceId)
				.setParameter("partitionIds", thrValues)
				.setMaxResults(1)
				.getResultList()
				.stream()
				.findFirst();
	}

	public List<ResourceTable> findAllByIdAndLoadForcedIds(List<Long> thePids) {
		return entityManager
				.createQuery(
						"SELECT t FROM ResourceTable t LEFT JOIN FETCH t.myForcedId WHERE t.myId IN :pids",
						ResourceTable.class)
				.setParameter("pids", thePids)
				.getResultList();
	}
}
