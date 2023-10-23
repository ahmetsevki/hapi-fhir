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

import ca.uhn.fhir.jpa.model.entity.ResourceHistoryTable;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

import java.util.*;

public class IResourceHistoryTableDao extends BaseDao<ResourceHistoryTable> {

	@PersistenceContext
	private EntityManager entityManager;

	public IResourceHistoryTableDao() {
		super(ResourceHistoryTable.class);
	}
	/**
	 * This is really only intended for unit tests - There can be many versions of resources in
	 * the real world, use a pageable query for real uses.
	 */
	public List<ResourceHistoryTable> findAllVersionsForResourceIdInOrder(Long theId) {
		return entityManager
				.createQuery(
						"SELECT t FROM ResourceHistoryTable t WHERE t.myResourceId = :resId ORDER BY t.myResourceVersion ASC",
						ResourceHistoryTable.class)
				.setParameter("resId", theId)
				.getResultList();
	}

	public ResourceHistoryTable findForIdAndVersionAndFetchProvenance(long theId, long theVersion) {
		return entityManager
				.createQuery(
						"SELECT t FROM ResourceHistoryTable t LEFT OUTER JOIN FETCH t.myProvenance WHERE t.myResourceId = :id AND t.myResourceVersion = :version",
						ResourceHistoryTable.class)
				.setParameter("id", theId)
				.setParameter("version", theVersion)
				.getSingleResult();
	}

	public List<Long> findForResourceId(Long theId, Long theDontWantVersion) {
		return entityManager
				.createQuery(
						"SELECT t.myId FROM ResourceHistoryTable t WHERE t.myResourceId = :resId AND t.myResourceVersion <> :dontWantVersion",
						Long.class)
				.setParameter("resId", theId)
				.setParameter("dontWantVersion", theDontWantVersion)
				.getResultList();
	}

	public List<ResourceHistoryTable> findForResourceIdAndReturnEntitiesAndFetchProvenance(
			Long theId, Long theDontWantVersion) {
		return entityManager
				.createQuery(
						"SELECT t FROM ResourceHistoryTable t LEFT OUTER JOIN FETCH t.myProvenance WHERE t.myResourceId = :resId AND t.myResourceVersion <> :dontWantVersion",
						ResourceHistoryTable.class)
				.setParameter("resId", theId)
				.setParameter("dontWantVersion", theDontWantVersion)
				.getResultList();
	}

	public List<Long> findIdsOfPreviousVersionsOfResourceId(Long theResourceId) {
		return entityManager
				.createQuery(
						"" + "SELECT v.myId FROM ResourceHistoryTable v "
								+ "LEFT OUTER JOIN ResourceTable t ON (v.myResourceId = t.myId) "
								+ "WHERE v.myResourceVersion <> t.myVersion AND "
								+ "t.myId = :resId",
						Long.class)
				.setParameter("resId", theResourceId)
				.getResultList();
	}

	public List<Long> findIdsOfPreviousVersionsOfResources(String theResourceName) {
		return entityManager
				.createQuery(
						"" + "SELECT v.myId FROM ResourceHistoryTable v "
								+ "LEFT OUTER JOIN ResourceTable t ON (v.myResourceId = t.myId) "
								+ "WHERE v.myResourceVersion <> t.myVersion AND "
								+ "t.myResourceType = :restype",
						Long.class)
				.setParameter("restype", theResourceName)
				.getResultList();
	}

	public List<Long> findIdsOfPreviousVersionsOfResources() {
		return entityManager
				.createQuery(
						"" + "SELECT v.myId FROM ResourceHistoryTable v "
								+ "LEFT OUTER JOIN ResourceTable t ON (v.myResourceId = t.myId) "
								+ "WHERE v.myResourceVersion <> t.myVersion",
						Long.class)
				.getResultList();
	}

	public void updateVersion(long theId, long theOldVersion, long theNewVersion) {
		entityManager
				.createQuery(
						"UPDATE ResourceHistoryTable r SET r.myResourceVersion = :newVersion WHERE r.myResourceId = :id AND r.myResourceVersion = :oldVersion")
				.setParameter("id", theId)
				.setParameter("oldVersion", theOldVersion)
				.setParameter("newVersion", theNewVersion)
				.executeUpdate();
	}

	public void deleteByPid(Long theId) {
		entityManager
				.createQuery("DELETE FROM ResourceHistoryTable t WHERE t.myId = :pid")
				.setParameter("pid", theId)
				.executeUpdate();
	}
}
