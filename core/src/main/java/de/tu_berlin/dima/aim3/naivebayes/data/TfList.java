package de.tu_berlin.dima.aim3.naivebayes.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactString;

public class TfList implements Value{
	HashMap<PactString, Integer> featureMap = new HashMap<PactString, Integer>();
	
	public void put(PactString token, int count) {
		featureMap.put(token, count);
	}
	
	public int get(PactString token) {
		return featureMap.get(token);
	}
	
	public boolean containsKey(PactString token) {
		return featureMap.containsKey(token);
	}
	
	public Set<Entry<PactString, Integer>> entrySet() {
		return featureMap.entrySet();
	}

	@Override
	public void read(DataInput in) throws IOException {
		int count = in.readInt();
		
		for (int i = 0; i < count; i++) {
			PactString token = new PactString();
			token.read(in);
			int tf = in.readInt();
			featureMap.put(token, tf);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(featureMap.size());
		
		for (Entry<PactString, Integer> entry : featureMap.entrySet()) {
			entry.getKey().write(out);
			out.writeInt(entry.getValue());
		}
	}

}
