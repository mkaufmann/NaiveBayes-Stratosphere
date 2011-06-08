package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Iterator;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactString;

public class LabelAggregator extends ReduceStub<PactString, FeatureList, PactString, FeatureList> {

	@Override
	public void reduce(PactString key, Iterator<FeatureList> values,
			Collector<PactString, FeatureList> out) {
		FeatureList value = new FeatureList();
		while (values.hasNext())
		{
			value.addAll(values.next());
		}
		out.collect(key, value);
	}

}
