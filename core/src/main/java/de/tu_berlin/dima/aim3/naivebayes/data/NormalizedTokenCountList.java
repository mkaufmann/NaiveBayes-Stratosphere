package de.tu_berlin.dima.aim3.naivebayes.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactString;

public class NormalizedTokenCountList implements Value{
	double lengthNormalized;
	HashMap<PactString, Integer> tokenList = new HashMap<PactString, Integer>();
	
	
	public double getLengthNormalized() {
		return lengthNormalized;
	}

	public void setLengthNormalized(double lengthNormalized) {
		this.lengthNormalized = lengthNormalized;
	}
	
	public void put(PactString token, int count) {
		tokenList.put(token, count);
	}
	
	public int get(PactString token) {
		return tokenList.get(token);
	}
	
	public boolean containsKey(PactString token) {
		return tokenList.containsKey(token);
	}
	
	public Set<Entry<PactString, Integer>> entrySet() {
		return tokenList.entrySet();
	}

	@Override
	public void read(DataInput in) throws IOException {
		lengthNormalized = in.readDouble();
		int count = in.readInt();
		
		for (int i = 0; i < count; i++) {
			PactString token = new PactString();
			token.read(in);
			int tokenCount = in.readInt();
			tokenList.put(token, tokenCount);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(lengthNormalized);
		out.writeInt(tokenList.size());
		
		for (Entry<PactString, Integer> entry : tokenList.entrySet()) {
			entry.getKey().write(out);
			out.writeInt(entry.getValue());
		}
	}

}
