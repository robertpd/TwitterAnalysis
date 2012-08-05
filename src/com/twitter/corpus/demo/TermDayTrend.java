package com.twitter.corpus.demo;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;

import com.twitter.corpus.analysis.Admin;

public class TermDayTrend {
	private static final Logger LOG = Logger.getLogger(TermDayTrend.class);
	public static int defaultNumTerms = 200;
	public final static Pattern lePattern = Pattern.compile("[^AB]\\s[0-9]{2}");
	// Store trends
//	public class TermTrend implements Serializable{
//		
//		/**
//		 * 
//		 */
//		private static final long serialVersionUID = 4678289703100226412L;
//		public TermTrend(Term t, int[] tr){
//			term = t;
//			trend = tr;
//		}
//		private Term term;
//		private int[] trend;
//	}
	// Method takes a ranked list of terms for which trend are calculated.
	public TermTrend[] getTrend(IndexReader reader, TermInfo[] rankedTerms) throws Exception {
		if (reader == null) return null;
		long startTime = System.currentTimeMillis();
		int c = 0;
		TermTrend[] termTrends = new TermTrend[rankedTerms.length];
		for(TermInfo t: rankedTerms){
			Term term = t.term;
//			long readerTime = System.currentTimeMillis();
			TermDocs termDocs = reader.termDocs(term);
			Long termTrendStart = System.currentTimeMillis();
//			LOG.info("Reader access time for term " + term.text() + " : " + Admin.getTime(readerTime, termTrendStart));
			int[] frequencies = new int[17];
			while(termDocs.next()){
				Document doc = reader.document(termDocs.doc());
				String d = doc.getFieldable("day").stringValue();
				int dayNum = Integer.parseInt(d.split("_")[1]);
				int index;
				if(d.startsWith("Jan")){
					index = dayNum - 23;
				}else if(d.startsWith("Feb")){
					index = dayNum + 8;
				}else{
					index = 100;
				}
				if(index < 0 || index > 16){
//					LOG.info("Out of Bounds Day: expected day between Jan 23rd and Feb 8th, instead: " + d);
				}
				if(!(index < 0 || index > 16)){
					frequencies[index]++;
				}
			}
			Long termTrendFin = System.currentTimeMillis();
			LOG.info(t.term.field() + " term [" + Integer.toString(c + 1) +"] " + t.term.text() + " has been trended in " + Admin.getTime(termTrendStart, termTrendFin));
			termTrends[c] = new TermTrend(term, frequencies);
//			termTrends.add(new TermTrend(term, frequencies));
			c++;
		}
		LOG.info("Finished Trending. Took " + Admin.getTime(startTime, System.currentTimeMillis()));
	return termTrends;
	}
}