package de.tu_berlin.dima.aim3.naivebayes.classifier;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.mahout.classifier.bayes.common.BayesParameters;
import org.apache.mahout.classifier.bayes.datastore.InMemoryBayesDatastore;
import org.apache.mahout.classifier.bayes.exceptions.InvalidDatastoreException;

import de.tu_berlin.dima.aim3.naivebayes.data.Feature;
import de.tu_berlin.dima.aim3.naivebayes.data.Label;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesInputFormats.FeatureSumInputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesInputFormats.IdfInputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesInputFormats.LabelSumInputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesInputFormats.ThetaNormalizedInputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesInputFormats.TotalSumInputFormat;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactNull;

public class PactBayesDatastore extends InMemoryBayesDatastore {

	public final static String SIGMA_J_DEFAULT_PATH = "/trainer-weights/Sigma_j/";
	public final static String SIGMA_K_DEFAULT_PATH = "/trainer-weights/Sigma_k/";
	public final static String SIGMA_K_SIGMA_J_DEFAULT_PATH = "/trainer-weights/Sigma_kSigma_j/";
	public final static String THETA_NORMALIZER_DEFAULT_PATH = "/trainer-thetaNormalizer/";
	public final static String WEIGHT_DEFAULT_PATH = "/trainer-tfIdf/";
	
	private String modelBasePath;
	
	public PactBayesDatastore(BayesParameters params) {
		super(params);
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
			super.loadFeatureWeight(labelTokenPair.getSecond().toString(), 
					labelTokenPair.getFirst().toString(), weight.getValue());	
			//LOG.info("AAA Feature Weight: " + labelTokenPair.getFirst().toString() + " : " + labelTokenPair.getSecond().toString() + " :: " + weight.getValue());
			labelTokenPair = new LabelFeaturePair();
			weight = new PactDouble();
		}
	}

	private void loadThetaNormalizer(String path) throws IOException, URISyntaxException {
		ThetaNormalizedInputFormat input = new ThetaNormalizedInputFormat(path);
		Label label = new Label();
		PactDouble weight = new PactDouble();
		while (input.readPair(label, weight))
		{
			super.setThetaNormalizer(label.toString(), weight.getValue());	
			//LOG.info("AAA Theta Normalized: " + label.getValue() + " :: " + weight.getValue());
			label = new Label();
			weight = new PactDouble();
		}
	}

	public void loadSumWeight(String path) throws IOException, URISyntaxException {
		TotalSumInputFormat input = new TotalSumInputFormat(path);
		PactNull pactNull = PactNull.getInstance();
		PactDouble sigmaJsigmaK = new PactDouble();
		while (input.readPair(pactNull, sigmaJsigmaK))
		{
			super.setSigmaJSigmaK(sigmaJsigmaK.getValue());
			//System.out.println("SigmaJSigmaK: " + sigmaJsigmaK);
			sigmaJsigmaK = new PactDouble();
		}
		
	}

	private void loadLabelWeights(String path) throws IOException, URISyntaxException {
		LabelSumInputFormat input = new LabelSumInputFormat(path);
		Label label = new Label();
		PactDouble weight = new PactDouble();
		while (input.readPair(label, weight))
		{
			super.setSumLabelWeight(label.toString(), weight.getValue());	
			//System.out.println("Sum Label Weight: " + label.getValue() + " :: " + weight.getValue());
			label = new Label();
			weight = new PactDouble();
		}
	}

	private void loadFeatureWeights(String path) throws IOException, URISyntaxException {
		FeatureSumInputFormat input = new FeatureSumInputFormat(path);
		Feature feature = new Feature();
		PactDouble weight = new PactDouble();
		while (input.readPair(feature, weight))
		{
			super.setSumFeatureWeight(feature.toString(), weight.getValue());	
			//System.out.println("Sum Feature Weight: " + feature.getValue() + " :: " + weight.getValue());
			feature = new Feature();
			weight = new PactDouble();
		}
	}
}
