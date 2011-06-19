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

package de.tu_berlin.dima.aim3.naivebayes.classifier;

import java.util.Iterator;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelPair;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Summing reducer
 * 
 * @author mkaufmann
 *
 */
public class ClassifyingReducer extends ReduceStub<LabelPair, PactInteger, LabelPair, PactInteger> {

	@Override
	public void reduce(LabelPair labels, Iterator<PactInteger> counts,
			Collector<LabelPair, PactInteger> out) {
		int sum = 0;
		
	    while (counts.hasNext()) {
	      sum += counts.next().getValue();
	    }
	    
	    out.collect(labels, new PactInteger(sum));
	}

}
