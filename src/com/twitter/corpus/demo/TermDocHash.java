package com.twitter.corpus.demo;

import java.util.HashSet;

public class TermDocHash implements java.io.Serializable{
	private static final long serialVersionUID = 2381362361557552107L;
	TermDocHash(){
		this.docHash = new HashSet<Long>();
	}
	public HashSet<Long> docHash;
}
