/**
 * Copyright (C) 2011 AIM III course DIMA TU Berlin
 *
 * This programm is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tu_berlin.dima.aim3.naivebayes.classifier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

	
	private static final Log LOG = LogFactory.getLog(ClassifyingMapper.class);
	
	private static final PactInteger ONE = new PactInteger(1);
	public static final String MODEL_BASE_PATH = "bayes.model.base.path";
	
	private boolean firstCall = true;
	private String modelBasePath;
	
	private int gramSize = 1;
	private ClassifierContext classifier;
	private String defaultCategory;

	@Override
	public void map(Label correctLabel, FeatureList features,
			Collector<LabelPair, PactInteger> out) {
		
		if (firstCall)
		{
			LOG.info("Reading model.");
			try {
				Algorithm algorithm = new BayesAlgorithm();
				// TODO: Support cbayes

				BayesParameters params = new BayesParameters();
				params.setBasePath(modelBasePath);
				Datastore datastore = new PactBayesDatastore(params);

				classifier = new ClassifierContext(algorithm, datastore);
				classifier.initialize();

				// defaultCategory = parameters.getString("", "");
				// gramSize = params.getGramSize();
			} catch (InvalidDatastoreException e) {
			}
			LOG.info("Reading model finished");
			firstCall = false;
		}
		
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
		modelBasePath = conf.getString(MODEL_BASE_PATH, "");
		firstCall = true;
	}

	
}
