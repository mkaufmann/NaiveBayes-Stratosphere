package de.tu_berlin.dima.aim3.naivebayes.classifier;

import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.bayes.algorithm.BayesAlgorithm;
import org.apache.mahout.classifier.bayes.common.BayesParameters;
import org.apache.mahout.classifier.bayes.exceptions.InvalidDatastoreException;
import org.apache.mahout.classifier.bayes.interfaces.Algorithm;
import org.apache.mahout.classifier.bayes.interfaces.Datastore;
import org.apache.mahout.classifier.bayes.model.ClassifierContext;

import de.tu_berlin.dima.aim3.naivebayes.data.Feature;
import de.tu_berlin.dima.aim3.naivebayes.data.FeatureList;
import de.tu_berlin.dima.aim3.naivebayes.data.Label;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelPair;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class ClassifyingMapper extends MapStub<Label, FeatureList, LabelPair, PactInteger> {

	private static final PactInteger ONE = new PactInteger(1);
	public static final String MODEL_BASE_PATH = "bayes.model.base.path";
	
	private int gramSize = 1;
	private ClassifierContext classifier;
	private String defaultCategory;

	@Override
	public void map(Label correctLabel, FeatureList features,
			Collector<LabelPair, PactInteger> out) {
		
		//TODO: Use gramsSize
		String[] document = new String[features.size()];
		int i = 0;
		for (Feature feature : features) {
			document[i++] = feature.toString();
		}
		
		try {
			ClassifierResult result = classifier.classifyDocument(document, defaultCategory);
			
			LabelPair labels = new LabelPair();
			labels.setFirst(correctLabel);
			labels.setSecond(new Label(result.getLabel().getBytes()));

			out.collect(labels, ONE);
		} catch (InvalidDatastoreException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void configure(Configuration conf) {
		super.configure(conf);
		
		try {
			Algorithm algorithm = new BayesAlgorithm();
			// TODO: Support cbayes

			BayesParameters params = new BayesParameters();
			params.setBasePath(conf.getString(MODEL_BASE_PATH, ""));
			Datastore datastore = new PactBayesDatastore(params);

			classifier = new ClassifierContext(algorithm, datastore);
			classifier.initialize();

			// defaultCategory = parameters.getString("", "");
			// gramSize = params.getGramSize();
		} catch (InvalidDatastoreException e) {
		}
	}

	
}
