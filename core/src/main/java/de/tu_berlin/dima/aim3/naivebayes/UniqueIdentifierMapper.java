package de.tu_berlin.dima.aim3.naivebayes;

import java.util.HashSet;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class UniqueIdentifierMapper extends MapStub<PactString, FeatureList, PactInteger, PactString> {

	private final static PactInteger PACT_ONE = new PactInteger(1);
	private HashSet<PactString> hashSet = new HashSet<PactString>();

	@Override
	public void map(PactString key, FeatureList value,
			Collector<PactInteger, PactString> out) {
		if (!hashSet.contains(key))
		{
			hashSet.add(key);
			out.collect(PACT_ONE, key);
		}
	}

}
