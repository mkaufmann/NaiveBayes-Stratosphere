package de.tu_berlin.dima.aim3.naivebayes.io;

import java.io.IOException;
import java.net.URISyntaxException;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelTokenPair;
import eu.stratosphere.pact.common.type.base.PactDouble;

public class IdfInputFormat extends BinaryInputFormat<LabelTokenPair, PactDouble> {

	public IdfInputFormat(String path) throws IOException, URISyntaxException {
		super(path);
	}

}
