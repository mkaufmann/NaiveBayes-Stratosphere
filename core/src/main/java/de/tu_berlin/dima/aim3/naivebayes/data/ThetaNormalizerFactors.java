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
