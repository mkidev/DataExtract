package edu.mki.bachelor.dataimport;

import org.apache.commons.math3.ml.clustering.Clusterable;

public class WordWrapper implements Clusterable {
	private double[] points;
	private WordAtDateData word;
	private double x;

	public WordWrapper(WordAtDateData word, double x) {
		this.word = word;
		this.x = x;
		this.points = new double[] { (double) x, word.getFreq() };
	}

	@Override
	public double[] getPoint() {
		return points;
	}

	public WordAtDateData getWord() {
		return word;
	}

	public double getX() {
		return x;
	}

}
