package de.tu_berlin.dima.aim3.naivebayes;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class OverallWordCountMapper extends MapStub<PactString, PactDouble, PactInteger, PactInteger> {

	private final static PactInteger ONE = new PactInteger(1);
	
	@Override
	public void map(PactString word, PactDouble wordOccurences,
			Collector<PactInteger, PactInteger> out) {
		out.collect(ONE, ONE);
	}

}
