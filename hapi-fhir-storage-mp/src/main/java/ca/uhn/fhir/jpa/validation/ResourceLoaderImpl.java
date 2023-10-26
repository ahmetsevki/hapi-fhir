/*-
 * #%L
 * HAPI FHIR Storage api
 * %%
 * Copyright (C) 2014 - 2023 Smile CDR, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package ca.uhn.fhir.jpa.validation;

import ca.uhn.fhir.jpa.api.dao.DaoRegistry;
import ca.uhn.fhir.rest.api.server.SystemRequestDetails;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.validation.IResourceLoader;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;

@RequestScoped
public class ResourceLoaderImpl implements IResourceLoader {
	@Inject
	DaoRegistry myDaoRegistry;

	@Override
	public <T extends IBaseResource> T load(Class<T> theType, IIdType theId) throws ResourceNotFoundException {
		SystemRequestDetails systemRequestDetails = SystemRequestDetails.forAllPartitions();
		return myDaoRegistry.getResourceDao(theType).read(theId, systemRequestDetails);
	}
}