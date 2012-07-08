package com.twitter.corpus.demo;

import java.io.Serializable;

import org.apache.lucene.index.Term;

public class TermInfo implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4698209047795941060L;
	public Term term;
	public int docFreq;

	public TermInfo(Term t, int df) {
		this.term = t;
		this.docFreq = df;
	}
}