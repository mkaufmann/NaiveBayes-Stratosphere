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

package de.tu_berlin.dima.aim3.naivebayes.data;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactPair;

public class LabelFeaturePair extends PactPair<Label, Feature>{

	public LabelFeaturePair(Label label, Feature feature) {
		super(label,feature);
	}
	
	public LabelFeaturePair(){
		super();
	}
	
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof PactPair<?, ?>))
			throw new ClassCastException("Cannot compare "
					+ o.getClass().getName() + " to N_Pair.");

		int result = this.getSecond().compareTo(
				((PactPair<?, ?>) o).getSecond());
		if (result == 0)
			result = this.getFirst().compareTo(((PactPair<?, ?>) o).getFirst());
		return result;
	}

}
