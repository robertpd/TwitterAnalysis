package com.twitter.corpus.demo;

import org.apache.lucene.util.PriorityQueue;

public class TermInfoQueue extends PriorityQueue<TermInfo> {
	TermInfoQueue(int size) {
		initialize(size);
	}
	protected final boolean lessThan(TermInfo a, TermInfo b) {
		
		return a.docFreq < b.docFreq;
	}
}
