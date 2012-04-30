package com.twitter.corpus.demo;

import java.io.Serializable;

import org.apache.lucene.index.Term;


public class TermTrend implements Serializable{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 4678289703100226412L;
		public TermTrend(Term t, int[] tr){
			term = t;
			trend = tr;
		}
		private Term term;
		private int[] trend;
	}
