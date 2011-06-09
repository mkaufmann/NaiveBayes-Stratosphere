package de.tu_berlin.dima.aim3.naivebayes.classifier;

import org.apache.mahout.classifier.bayes.common.BayesParameters;
import org.apache.mahout.classifier.bayes.datastore.InMemoryBayesDatastore;
import org.apache.mahout.classifier.bayes.exceptions.InvalidDatastoreException;

public class PactBayesDatastore extends InMemoryBayesDatastore {

	private BayesParameters params;
	
	public PactBayesDatastore(BayesParameters params) {
		super(params);
		this.params = params;
	}

	@Override
	public void initialize() throws InvalidDatastoreException {
		loadModel();
	}
	
	private void loadModel() {
		loadFeatureWeights(params.get("sigma_j"));
	    loadLabelWeights(params.get("sigma_k"));
	    loadSumWeight(params.get("sigma_kSigma_j"));
	    loadThetaNormalizer(params.get("thetaNormalizer"));
	    loadWeightMatrix(params.get("weight"));
	}

	private void loadWeightMatrix(String path) {
		// TODO Auto-generated method stub
		
	}

	private void loadThetaNormalizer(String path) {
		// TODO Auto-generated method stub
		
	}

	private void loadSumWeight(String path) {
		// TODO Auto-generated method stub
		
	}

	private void loadLabelWeights(String path) {
		// TODO Auto-generated method stub
		
	}

	private void loadFeatureWeights(String path) {
		// TODO Auto-generated method stub
		
	}
}
