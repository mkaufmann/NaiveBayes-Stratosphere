package de.tu_berlin.dima.aim3.naivebayes;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactString;

public class NaiveBayesWeightsMapper extends MapStub<PactInteger, PactList<PactString>, PactString, PactList<PactDouble>> {

	@Override
	public void map(PactInteger arg0, PactList<PactString> arg1,
			Collector<PactString, PactList<PactDouble>> arg2) {
		// TODO ????
		
	}

}
