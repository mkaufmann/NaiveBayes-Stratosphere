package de.tu_berlin.dima.aim3.naivebayes;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactString;

public class NaiveBayesInstanceMapper extends MapStub<PactString, PactList<PactString>, PactInteger, PactList<PactString>> {

	@Override
	public void map(PactString label, PactList<PactString> terms,
			Collector<PactInteger, PactList<PactString>> out) {
		//TODO: filter invalid labels and assign labels int ids for better efficiency
		//TODO: Label map is fixed for beginning
	}

}
