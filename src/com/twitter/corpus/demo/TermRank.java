package com.twitter.corpus.demo;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermEnum;

public class TermRank {
	private static final Logger LOG = Logger.getLogger(TermRank.class);
	public TermRank(){}
	// Gets the top n occurring terms of type {field}

	public TermInfo[] getHighFreqTerms(IndexReader reader, int defaultNumTerms, String fieldToRank) throws Exception {
		if (reader == null || fieldToRank == null) return null;
		LOG.info("Ranking top " + Integer.toString(defaultNumTerms) + " terms.");
		long startTime = System.currentTimeMillis();
		
		TermInfoQueue tiq = new TermInfoQueue(defaultNumTerms);
		TermEnum terms = reader.terms();
		int minFreq = 0;
		int c = 0;
		while (terms.next()) {
			String termField = terms.term().field();
			
			if (fieldToRank != null) {
				boolean skip = true;
				
				for (int i = 0; i < 1; i++) {
					if (termField.equals(fieldToRank)) {
						skip = false;
						break;
					}
				}
				if (skip) continue;
			}
			if (terms.docFreq() > minFreq) {
				tiq.insertWithOverflow(new TermInfo(terms.term(), terms.docFreq()));
				if (tiq.size() >= defaultNumTerms) 		     // if tiq overfull
				{
					tiq.pop();				     // remove lowest in tiq
					minFreq = ((TermInfo)tiq.top()).docFreq; // reset minFreq
				}
			}
			c++;
		}
		TermInfo[] res = new TermInfo[tiq.size()];

		for (int i = 0; i < res.length; i++) {
			res[res.length - i - 1] = (TermInfo)tiq.pop();
		}
		LOG.info("Ranking terms took: " + Admin.getTime(startTime, System.currentTimeMillis()));
		return res;
	}
}