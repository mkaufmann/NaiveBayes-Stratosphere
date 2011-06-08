package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Iterator;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactString;

public class NaiveBayesSumReducer extends ReduceStub<PactInteger, PactList<PactString>, PactInteger, PactList<PactString>> {

	@Override
	public void reduce(PactInteger labelId, Iterator<PactList<PactString>> termLists,
			Collector<PactInteger, PactList<PactString>> out) {
		// TODO Combine term lists into one list
		
	}

}
