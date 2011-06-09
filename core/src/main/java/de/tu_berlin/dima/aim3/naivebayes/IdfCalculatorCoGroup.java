/*package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Iterator;

import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

public class IdfCalculatorCoGroup extends CoGroupStub<PactString, PactDouble, PactPair<PactString, PactDouble>, PactPair<PactString,PactString>, PactDouble> {

	public void match(PactString label, PactDouble documentsPerLabel, PactPair<PactString, PactDouble> trainerDocCount,
			Collector<PactPair<PactString,PactString>, PactDouble> out) {
        Double labelDocumentCount = documentsPerLabel.getValue();
        double logIdf = Math.log(labelDocumentCount / trainerDocCount.getSecond().getValue());
        out.collect(new PactPair<PactString, PactString>(label,trainerDocCount.getFirst()) {}, new PactDouble(logIdf));
	}

	@Override
	public void coGroup(PactString label, Iterator<PactDouble> documentsPerLabelIt,
			Iterator<PactPair<PactString, PactDouble>> trainerDocCount,
			Collector<PactPair<PactString, PactString>, PactDouble> out) {
		if (documentsPerLabelIt.hasNext() && trainerDocCount.hasNext())
		{
			double documentsPerLabel = documentsPerLabelIt.next().getValue();
			
			double idfTimesDIJ = 1.0;
			
		//	while (trainerDocCount.)
			
		}
	}

}*/
