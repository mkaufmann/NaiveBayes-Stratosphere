/**
 * Copyright (C) 2011 AIM III course DIMA TU Berlin
 *
 * This programm is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tu_berlin.dima.aim3.naivebayes.deprecated;

import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactString;

public class NaiveBayesTrainer implements PlanAssembler {

	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		String trainFile = "file:///tmp/naive/train";
		String labelsFile = "file:///tmp/naive/labels";
		int gramSize = 1;
		
		if(args.length > 0) {
			trainFile = args[0];
		}
		if(args.length > 1) {
			labelsFile = args[1];
		}
		
		DataSourceContract<PactString, PactList<PactString>> trainSource =
			new DataSourceContract<PactString, PactList<PactString>>(null, trainFile);
		DataSourceContract<PactString, PactList<PactString>> labelSource =
			new DataSourceContract<PactString, PactList<PactString>>(null, labelsFile);
		
		MatchContract<PactString, PactList<PactString>, PactInteger, PactInteger, PactList<PactString>> instanceMatch = 
			new MatchContract<PactString, PactList<PactString>, PactInteger, PactInteger, PactList<PactString>>(NaiveBayesInstanceMatch.class);
		instanceMatch.setFirstInput(trainSource);
		instanceMatch.setSecondInput(labelSource);
		
		ReduceContract<PactInteger, PactList<PactString>, PactInteger, PactList<PactString>> sumReducer =
			new ReduceContract<PactInteger, PactList<PactString>, PactInteger, PactList<PactString>>(NaiveBayesSumReducer.class);
		sumReducer.setInput(instanceMatch);
		
		return null;
	}

}
