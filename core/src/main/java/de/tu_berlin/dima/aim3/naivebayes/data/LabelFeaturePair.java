package de.tu_berlin.dima.aim3.naivebayes.data;

import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

public class LabelFeaturePair extends PactPair<PactString, PactString>{

	public LabelFeaturePair(PactString label, PactString feature) {
		super(label,feature);
	}
	
	public LabelFeaturePair(){
		super();
	}

}