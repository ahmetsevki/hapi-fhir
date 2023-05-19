/*-
 * #%L
 * HAPI FHIR - Master Data Management
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
package ca.uhn.fhir.mdm.rules.svc;

import ca.uhn.fhir.context.ConfigurationException;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.i18n.Msg;
import ca.uhn.fhir.jpa.searchparam.matcher.IMdmFieldMatcher;
import ca.uhn.fhir.mdm.api.IMdmSettings;
import ca.uhn.fhir.mdm.api.MdmConstants;
import ca.uhn.fhir.mdm.api.MdmMatchEvaluation;
import ca.uhn.fhir.mdm.api.MdmMatchOutcome;
import ca.uhn.fhir.mdm.api.MdmMatchResultEnum;
import ca.uhn.fhir.mdm.log.Logs;
import ca.uhn.fhir.mdm.rules.json.MdmFieldMatchJson;
import ca.uhn.fhir.mdm.rules.json.MdmRulesJson;
import ca.uhn.fhir.mdm.rules.matcher.IMatcherFactory;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * The MdmResourceComparator is in charge of performing actual comparisons between left and right records.
 * It does so by calling individual comparators, and returning a vector based on the combination of
 * field comparators that matched.
 */

@Service
public class MdmResourceMatcherSvc {
	private static final Logger ourLog = Logs.getMdmTroubleshootingLog();

	private final FhirContext myFhirContext;
	private final IMatcherFactory myMatcherFactory;
	private MdmRulesJson myMdmRulesJson;
	private final List<MdmResourceFieldMatcher> myFieldMatchers = new ArrayList<>();

	public MdmResourceMatcherSvc(
		FhirContext theFhirContext,
		IMatcherFactory theIMatcherFactory,
		IMdmSettings theMdmSettings
	) {
		myFhirContext = theFhirContext;
		myMatcherFactory = theIMatcherFactory;
		myMdmRulesJson = theMdmSettings.getMdmRules();

		addFieldMatchers();
	}

	private void addFieldMatchers() {
		if (myMdmRulesJson == null) {
			throw new ConfigurationException(Msg.code(1521) + "Failed to load MDM Rules.  If MDM is enabled, then MDM rules must be available in context.");
		}
		myFieldMatchers.clear();
		for (MdmFieldMatchJson matchFieldJson : myMdmRulesJson.getMatchFields()) {
			myFieldMatchers.add(new MdmResourceFieldMatcher(myFhirContext, myMatcherFactory, matchFieldJson, myMdmRulesJson));
		}
	}

	/**
	 * Given two {@link IBaseResource}s, perform all comparisons on them to determine an {@link MdmMatchResultEnum}, indicating
	 * to what level the two resources are considered to be matching.
	 *
	 * @param theLeftResource  The first {@link IBaseResource}.
	 * @param theRightResource The second {@link IBaseResource}
	 * @return an {@link MdmMatchResultEnum} indicating the result of the comparison.
	 */
	public MdmMatchOutcome getMatchResult(IBaseResource theLeftResource, IBaseResource theRightResource) {
		return match(theLeftResource, theRightResource);
	}

	MdmMatchOutcome match(IBaseResource theLeftResource, IBaseResource theRightResource) {
		MdmMatchOutcome matchResult = getMatchOutcome(theLeftResource, theRightResource);
		MdmMatchResultEnum matchResultEnum = myMdmRulesJson.getMatchResult(matchResult.getVector());
		matchResult.setMatchResultEnum(matchResultEnum);
		if (ourLog.isDebugEnabled()) {
			ourLog.debug("{} {}: {}", matchResult.getMatchResultEnum(), theRightResource.getIdElement().toUnqualifiedVersionless(), matchResult);
			if (ourLog.isTraceEnabled()) {
				ourLog.trace("Field matcher results:\n{}", myMdmRulesJson.getDetailedFieldMatchResultWithSuccessInformation(matchResult.getVector()));
			}
		}
		return matchResult;
	}

	/**
	 * This function generates a `match vector`, which is a long representation of a binary string
	 * generated by the results of each of the given comparator matches. For example.
	 * start with a binary representation of the value 0 for long: 0000
	 * first_name matches, so the value `1` is bitwise-ORed to the current value (0) in right-most position.
	 * `0001`
	 * <p>
	 * Next, we look at the second field comparator, and see if it matches. If it does, we left-shift 1 by the index
	 * of the comparator, in this case also 1.
	 * `0010`
	 * <p>
	 * Then, we bitwise-or it with the current retval:
	 * 0001|0010 = 0011
	 * The binary string is now `0011`, which when you return it as a long becomes `3`.
	 */
	private MdmMatchOutcome getMatchOutcome(IBaseResource theLeftResource, IBaseResource theRightResource) {
		long vector = 0;
		double score = 0.0;
		int appliedRuleCount = 0;

		//TODO GGG MDM: This grabs ALL comparators, not just the ones we care about (e.g. the ones for Medication)
		String resourceType = myFhirContext.getResourceType(theLeftResource);

		for (int i = 0; i < myFieldMatchers.size(); ++i) {
			//any that are not for the resourceType in question.
			MdmResourceFieldMatcher fieldComparator = myFieldMatchers.get(i);
			if (!isValidResourceType(resourceType, fieldComparator.getResourceType())) {
				ourLog.debug("Matcher {} is not valid for resource type: {}. Skipping it.", fieldComparator.getName(), resourceType);
				continue;
			}
			ourLog.trace("Matcher {} is valid for resource type: {}. Evaluating match.", fieldComparator.getName(), resourceType);
			MdmMatchEvaluation matchEvaluation = fieldComparator.match(theLeftResource, theRightResource);
			if (matchEvaluation.match) {
				vector |= (1L << i);
				ourLog.trace("Match: Successfully matched matcher {} with score {}. New vector: {}", fieldComparator.getName(), matchEvaluation.score, vector);
			} else {
				ourLog.trace("No match: Matcher {} did not match (score: {}).", fieldComparator.getName(), matchEvaluation.score);
			}
			score += matchEvaluation.score;
			appliedRuleCount += 1;
		}

		MdmMatchOutcome retVal = new MdmMatchOutcome(vector, score);
		retVal.setMdmRuleCount(appliedRuleCount);
		return retVal;
	}


	private boolean isValidResourceType(String theResourceType, String theFieldComparatorType) {
		return (
			theFieldComparatorType.equalsIgnoreCase(MdmConstants.ALL_RESOURCE_SEARCH_PARAM_TYPE)
				|| theFieldComparatorType.equalsIgnoreCase(theResourceType)
		);
	}
}
