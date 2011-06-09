package de.tu_berlin.dima.aim3.naivebayes.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public class ThetaNormalizerFactors implements Value {
	double sigmaJSimgaK = -1;
	int vocabCount = -1;
	double labelWeight = -1;
	
	
	public double getSigmaJSimgaK() {
		return sigmaJSimgaK;
	}
	public void setSigmaJSimgaK(double sigmaJSimgaK) {
		this.sigmaJSimgaK = sigmaJSimgaK;
	}
	public int getVocabCount() {
		return vocabCount;
	}
	public void setVocabCount(int vocabCount) {
		this.vocabCount = vocabCount;
	}
	public double getLabelWeight() {
		return labelWeight;
	}
	public void setLabelWeight(double labelWeight) {
		this.labelWeight = labelWeight;
	}
	
	@Override
	public void read(DataInput in) throws IOException {
		sigmaJSimgaK = in.readDouble();
		vocabCount = in.readInt();
		labelWeight = in.readDouble();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(sigmaJSimgaK);
		out.writeInt(vocabCount);
		out.writeDouble(labelWeight);
	}
	
}
