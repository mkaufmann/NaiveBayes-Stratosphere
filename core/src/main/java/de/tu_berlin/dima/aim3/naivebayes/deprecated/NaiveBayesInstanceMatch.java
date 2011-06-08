package de.tu_berlin.dima.aim3.naivebayes.deprecated;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactString;

public class NaiveBayesInstanceMatch extends MatchStub<PactString, PactList<PactString>, PactInteger, PactInteger, PactList<PactString>> {

	@Override
	public void match(PactString label, PactList<PactString> terms,
			PactInteger labelId, Collector<PactInteger, PactList<PactString>> out) {
		out.collect(labelId, terms);
	}

}
