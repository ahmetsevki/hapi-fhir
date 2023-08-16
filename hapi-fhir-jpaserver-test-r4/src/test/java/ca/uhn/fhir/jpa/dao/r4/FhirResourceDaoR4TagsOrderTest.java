package ca.uhn.fhir.jpa.dao.r4;

import ca.uhn.fhir.jpa.api.config.JpaStorageSettings;
import ca.uhn.fhir.jpa.test.BaseJpaR4Test;
import ca.uhn.fhir.storage.interceptor.TagOrderInterceptor;
import ca.uhn.fhir.storage.test.TagTestCasesUtil;
import ca.uhn.fhir.util.MetaTagSorterAlphabetical;
import org.hl7.fhir.r4.model.Meta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static ca.uhn.fhir.test.utilities.TagTestUtil.createMeta;
import static ca.uhn.fhir.test.utilities.TagTestUtil.generateAllCodingPairs;

public class FhirResourceDaoR4TagsOrderTest extends BaseJpaR4Test {

	private TagOrderInterceptor myTagOrderInterceptor;

	private TagTestCasesUtil myTagTestCasesUtil;

	@Override
	@BeforeEach
	protected void before() throws Exception {
		super.before();
		MetaTagSorterAlphabetical tagSorter = new MetaTagSorterAlphabetical();
		myTagOrderInterceptor = new TagOrderInterceptor(tagSorter);
		myInterceptorRegistry.registerInterceptor(myTagOrderInterceptor);
		myTagTestCasesUtil = new TagTestCasesUtil(myPatientDao, mySrd);
	}

	@AfterEach
	protected void after() throws Exception {
		myInterceptorRegistry.unregisterInterceptor(myTagOrderInterceptor);
	}

	@ParameterizedTest
	@EnumSource(JpaStorageSettings.TagStorageModeEnum.class)
	public void testCreateResource_ExpectToRetrieveTagsSorted(JpaStorageSettings.TagStorageModeEnum tagStorageMode) {
		myStorageSettings.setTagStorageMode(tagStorageMode);
		myTagTestCasesUtil.createResourceWithTagsAndExpectToRetrieveThemSorted();
	}

	@ParameterizedTest
	@EnumSource(JpaStorageSettings.TagStorageModeEnum.class)
	public void testUpdateResource_ShouldNotIncreaseVersionBecauseOfTagOrder(JpaStorageSettings.TagStorageModeEnum tagStorageMode) {
		myStorageSettings.setTagStorageMode(tagStorageMode);
		myTagTestCasesUtil.updateResourceWithExistingTagsButInDifferentOrderAndExpectVersionToRemainTheSame();
	}

	@ParameterizedTest
	@EnumSource(
		value = JpaStorageSettings.TagStorageModeEnum.class,
		names = {"INLINE"},
		mode = EnumSource.Mode.EXCLUDE)
	public void testUpdateResource_ExpectToRetrieveTagsSorted_NonInlineModes(JpaStorageSettings.TagStorageModeEnum tagStorageMode) {
		myStorageSettings.setTagStorageMode(tagStorageMode);
		myTagTestCasesUtil.updateResourceWithTagsAndExpectNonInlineModeUpdateBehaviourAndExpectToRetrieveTagsSorted();
	}

	@Test
	public void testUpdateResource_ExpectToRetrieveTagsSorted_InlineMode() {
		myStorageSettings.setTagStorageMode(JpaStorageSettings.TagStorageModeEnum.INLINE);

		Meta metaInputOnCreate = createMeta(
			generateAllCodingPairs(List.of("sys2", "sys1"), List.of("c", "b", "a")), //tag
			generateAllCodingPairs(List.of("sys2", "sys1"), List.of("c", "b", "a")), //security
			List.of("c", "b", "a") // profile
		);

		// meta input for update (adding new tags)
		Meta metaInputOnUpdate = createMeta(
				generateAllCodingPairs(List.of("sys2", "sys1"), List.of("cc", "bb", "aa")), //tag
				generateAllCodingPairs(List.of("sys2", "sys1"), List.of("cc", "bb", "aa")), //security
				List.of("cc", "bb", "aa") //profile
			);

		// inline mode replaces the tags completely on update, so only new tags are expected after update
		Meta expectedMetaAfterUpdate = createMeta(
				generateAllCodingPairs(List.of("sys1", "sys2"), List.of("aa", "bb", "cc")), //tag (replaced & sorted)
				generateAllCodingPairs(List.of("sys1", "sys2"), List.of("aa", "bb", "cc")), //security (replaced & sorted)
				List.of("aa", "bb", "cc") //profile (replaced & sorted)
			);

		myTagTestCasesUtil.updateResourceAndVerifyMeta(metaInputOnCreate,  metaInputOnUpdate,expectedMetaAfterUpdate, false);
	}
}
