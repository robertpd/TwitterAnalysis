package com.twitter.corpus.types;

import java.io.Serializable;

public class CoWeight implements Serializable, Comparable<CoWeight> {
	private static final long serialVersionUID = 5864579699695542248L;
	public CoWeight(int t, double c){
		this.termId = t;
		this.correlate = c;
	}
	public int termId;
	public double correlate;
	public Double getCorrelate(){
		return this.correlate;
	}
	@Override
	public int compareTo(CoWeight other) {
		if(this.correlate > other.correlate)
			return -1;
		else if(this.correlate < other.correlate)
			return 1;
		else
			return 0;
	}
	@Override
	public boolean equals(Object b){
		if(b == null || b.getClass() != this.getClass()){
			return false;
		}
		if(((CoWeight)b).termId== this.termId && ((CoWeight)b).correlate == this.correlate){
			return true;
		}
		return false;
	}
	@Override
	public int hashCode(){
		// term should be unique
		return this.termId;
	}
}
