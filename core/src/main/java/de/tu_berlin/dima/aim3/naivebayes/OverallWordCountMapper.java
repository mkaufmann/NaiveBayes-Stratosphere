package de.tu_berlin.dima.aim3.naivebayes;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class OverallWordCountMapper extends MapStub<PactString, PactInteger, PactInteger, PactInteger> {

	private final static PactInteger ONE = new PactInteger(1);
	
	@Override
	public void map(PactString word, PactInteger wordOccurences,
			Collector<PactInteger, PactInteger> out) {
		out.collect(ONE, ONE);
	}

}
