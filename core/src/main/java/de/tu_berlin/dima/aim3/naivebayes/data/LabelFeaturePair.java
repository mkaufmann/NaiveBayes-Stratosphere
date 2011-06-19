package de.tu_berlin.dima.aim3.naivebayes.data;

import eu.stratosphere.pact.common.type.base.PactPair;

public class LabelFeaturePair extends PactPair<Label, Feature>{

	public LabelFeaturePair(Label label, Feature feature) {
		super(label,feature);
	}
	
	public LabelFeaturePair(){
		super();
	}

}
