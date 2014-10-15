package edu.mki.bachelor.dataimport;

public class WordAtDateData {

	private Float freq = 0F;
	private boolean peak = false;
	private String peakComment = "";
	private long dateLong = 0L;
	private int rank = 0;

	public WordAtDateData() {

	}

	public WordAtDateData(Float freq, Boolean peak, String peakComment) {
		this.setFreq(freq);
		this.setPeak(peak);
		this.setPeakComment(peakComment);
	}

	public WordAtDateData(Float freq, Boolean peak) {
		this.setFreq(freq);
		this.setPeak(peak);
		this.setPeakComment("");
	}

	public WordAtDateData(Float freq, int rank) {
		this.setFreq(freq);
		this.setPeak(peak);
		this.setPeakComment("");
		this.setRank(rank);
	}

	public WordAtDateData(Float freq) {
		this.setFreq(freq);
		this.setPeak(false);
		this.setPeakComment("");
	}

	public WordAtDateData(Float freq, long time) {
		this.setFreq(freq);
		this.setPeak(false);
		this.setPeakComment("");
		this.setDateLong(time);
	}

	public Float getFreq() {
		return freq;
	}

	public void setFreq(Float freq) {
		this.freq = freq;
	}

	public boolean isPeak() {
		return peak;
	}

	public void setPeak(boolean peak) {
		this.peak = peak;
	}

	public String getPeakComment() {
		return peakComment;
	}

	public void setPeakComment(String peakComment) {
		this.peakComment = peakComment;
	}

	public long getDateLong() {
		return dateLong;
	}

	public void setDateLong(long dateLong) {
		this.dateLong = dateLong;
	}

	public int getRank() {
		return rank;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}

}
