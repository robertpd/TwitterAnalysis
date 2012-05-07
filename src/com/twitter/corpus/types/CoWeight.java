package com.twitter.corpus.types;

import java.io.Serializable;

public class CoWeight implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5864579699695542248L;
	public CoWeight(int t, double c){
		this.termId = t;
		this.correlate = c;
	}
	public int termId;
	public double correlate;

//	public void clear(){
//		this.termId = 0;
//		this.correlate = 0.0;
//	}
}
