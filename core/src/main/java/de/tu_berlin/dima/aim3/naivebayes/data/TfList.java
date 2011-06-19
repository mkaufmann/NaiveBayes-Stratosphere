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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.type.Value;

public class TfList implements Value{
	HashMap<Feature, Integer> featureMap = new HashMap<Feature, Integer>();
	
	public void put(Feature feature, int count) {
		featureMap.put(feature, count);
	}
	
	public int get(Feature feature) {
		return featureMap.get(feature);
	}
	
	public boolean containsKey(Feature feature) {
		return featureMap.containsKey(feature);
	}
	
	public Set<Entry<Feature, Integer>> entrySet() {
		return featureMap.entrySet();
	}

	@Override
	public void read(DataInput in) throws IOException {
		int count = in.readInt();
		
		for (int i = 0; i < count; i++) {
			Feature feature = new Feature();
			feature.read(in);
			int tf = in.readInt();
			featureMap.put(feature, tf);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(featureMap.size());
		
		for (Entry<Feature, Integer> entry : featureMap.entrySet()) {
			entry.getKey().write(out);
			out.writeInt(entry.getValue());
		}
	}

}
