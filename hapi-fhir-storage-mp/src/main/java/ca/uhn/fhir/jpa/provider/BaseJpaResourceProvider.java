package ca.uhn.fhir.jpa.provider;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jaxrs.server.AbstractJaxRsResourceProvider;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.rest.annotation.*;
import ca.uhn.fhir.rest.api.MethodOutcome;
import jakarta.inject.Inject;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;

public abstract class BaseJpaResourceProvider<T extends IBaseResource> extends AbstractJaxRsResourceProvider<T> {
	private static final org.slf4j.Logger ourLog = org.slf4j.LoggerFactory.getLogger(BaseJpaResourceProvider.class);

	private IFhirResourceDao<T> myDao;

	@Inject
	public BaseJpaResourceProvider(FhirContext myContext) {
		super(myContext);
	}

	public IFhirResourceDao<T> getDao() {
		return myDao;
	}

	public void setDao(IFhirResourceDao<T> theDao) {
		myDao = theDao;
	}

	@Create
	public MethodOutcome create(@ResourceParam T theResource, @ConditionalUrlParam String theConditional) {
		if (theConditional != null) {
			throw new RuntimeException("not implemented");
		} else {
			return getDao().create(theResource);
		}
	}

	@Update
	public MethodOutcome update(
			@ResourceParam T theResource, @IdParam IIdType theId, @ConditionalUrlParam String theConditional) {
		if (theConditional != null) {
			throw new RuntimeException("not implemented");
		} else {
			return getDao().update(theResource);
		}
	}

	@Read
	public T read(@IdParam IIdType theId) {
		return myDao.read(theId);
	}
}
