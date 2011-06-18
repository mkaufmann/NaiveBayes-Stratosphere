package de.tu_berlin.dima.aim3.naivebayes.classifier;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.mahout.classifier.bayes.common.BayesParameters;
import org.apache.mahout.classifier.bayes.datastore.InMemoryBayesDatastore;
import org.apache.mahout.classifier.bayes.exceptions.InvalidDatastoreException;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesInputFormats.IdfInputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesInputFormats.ThetaNormalizedInputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesInputFormats.WeightInputFormat;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

public class PactBayesDatastore extends InMemoryBayesDatastore {

	public final static String SIGMA_J_DEFAULT_PATH = "/trainer-weights/Sigma_j/";
	public final static String SIGMA_K_DEFAULT_PATH = "/trainer-weights/Sigma_k/";
	public final static String SIGMA_K_SIGMA_J_DEFAULT_PATH = "/trainer-weights/Sigma_kSigma_j/";
	public final static String THETA_NORMALIZER_DEFAULT_PATH = "/trainer-thetaNormalizer/";
	public final static String WEIGHT_DEFAULT_PATH = "/trainer-tfIdf/";
	
	private String modelBasePath;
	private BayesParameters params;
	
	public PactBayesDatastore(BayesParameters params) {
		super(params);
		this.params = params;
		modelBasePath = params.getBasePath();
	}

	@Override
	public void initialize() throws InvalidDatastoreException {
		loadModel();
	}
	
	private void loadModel() {
	    try {
			/*loadWeightMatrix(params.get("weight", modelBasePath + WEIGHT_DEFAULT_PATH));
			loadFeatureWeights(params.get("sigma_j", modelBasePath + SIGMA_J_DEFAULT_PATH));
		    loadLabelWeights(params.get("sigma_k", modelBasePath + SIGMA_K_DEFAULT_PATH));
		    loadSumWeight(params.get("sigma_kSigma_j", modelBasePath + SIGMA_K_SIGMA_J_DEFAULT_PATH));
		    loadThetaNormalizer(params.get("thetaNormalizer", modelBasePath + THETA_NORMALIZER_DEFAULT_PATH));*/
	    	loadWeightMatrix(modelBasePath + WEIGHT_DEFAULT_PATH);
			loadFeatureWeights(modelBasePath + SIGMA_J_DEFAULT_PATH);
		    loadLabelWeights(modelBasePath + SIGMA_K_DEFAULT_PATH);
		    loadSumWeight(modelBasePath + SIGMA_K_SIGMA_J_DEFAULT_PATH);
		    loadThetaNormalizer(modelBasePath + THETA_NORMALIZER_DEFAULT_PATH);
	    	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException, InvalidDatastoreException{
		BayesParameters params = new BayesParameters();
		params.setBasePath("file:///Users/Ringwald/naiveData/strato/model");
		//params.set(ClassifyingMapper.MODEL_BASE_PATH, );
		PactBayesDatastore dataStore = new PactBayesDatastore(params);
		dataStore.initialize();
		//dataStore.loadSumWeight("file:///Users/Ringwald/naiveData/outp" + SIGMA_K_SIGMA_J_DEFAULT_PATH);
	}

	private void loadWeightMatrix(String path) throws IOException, URISyntaxException {
		IdfInputFormat input = new IdfInputFormat(path);
		LabelFeaturePair labelTokenPair = new LabelFeaturePair();
		PactDouble weight = new PactDouble();
		while (input.readPair(labelTokenPair, weight))
		{
			super.loadFeatureWeight(labelTokenPair.getSecond().getValue(), 
					labelTokenPair.getFirst().getValue(), weight.getValue());	
			//System.out.println("Feature Weight: " + labelTokenPair.getFirst().getValue() + " : " + labelTokenPair.getSecond().getValue() + " :: " + weight.getValue());
			labelTokenPair = new LabelFeaturePair();
			weight = new PactDouble();
		}
	}

	private void loadThetaNormalizer(String path) throws IOException, URISyntaxException {
		ThetaNormalizedInputFormat input = new ThetaNormalizedInputFormat(path);
		PactString label = new PactString();
		PactDouble weight = new PactDouble();
		while (input.readPair(label, weight))
		{
			super.setThetaNormalizer(label.getValue(), weight.getValue());	
			//System.out.println("Theta Normalized: " + label.getValue() + " :: " + weight.getValue());
			label = new PactString();
			weight = new PactDouble();
		}
	}

	public void loadSumWeight(String path) throws IOException, URISyntaxException {
		WeightInputFormat input = new WeightInputFormat(path);
		PactString pactString = new PactString();
		PactDouble sigmaJsigmaK = new PactDouble();
		while (input.readPair(pactString, sigmaJsigmaK))
		{
			super.setSigmaJSigmaK(sigmaJsigmaK.getValue());
			//System.out.println("SigmaJSigmaK: " + sigmaJsigmaK);
			sigmaJsigmaK = new PactDouble();
		}
		
	}

	private void loadLabelWeights(String path) throws IOException, URISyntaxException {
		WeightInputFormat input = new WeightInputFormat(path);
		PactString label = new PactString();
		PactDouble weight = new PactDouble();
		while (input.readPair(label, weight))
		{
			super.setSumLabelWeight(label.getValue(), weight.getValue());	
			//System.out.println("Sum Label Weight: " + label.getValue() + " :: " + weight.getValue());
			label = new PactString();
			weight = new PactDouble();
		}
	}

	private void loadFeatureWeights(String path) throws IOException, URISyntaxException {
		WeightInputFormat input = new WeightInputFormat(path);
		PactString feature = new PactString();
		PactDouble weight = new PactDouble();
		while (input.readPair(feature, weight))
		{
			super.setSumFeatureWeight(feature.getValue(), weight.getValue());	
			//System.out.println("Sum Feature Weight: " + feature.getValue() + " :: " + weight.getValue());
			feature = new PactString();
			weight = new PactDouble();
		}
	}
}
