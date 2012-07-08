package com.twitter.corpus.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;

public class Invoke {
	private static final Logger Log = Logger.getLogger(TermDayTrend.class);
	
	private static TermRank termRank;
	private static IndexReader reader;
	private static int defaultNumTerms = 1;
	private static String field = "hash";
	private static TermTrend[] trendList;
	
	public static void main(String[] args) throws Exception{

		reader = IndexReader.open(FSDirectory.open(new File(args[0])));

		// GetrankedTerms
		termRank = new TermRank();
		//get the top ranking terms, returns TermFreq array: contains Term and its Freq.
//		TermInfo[] rankedTerms = getRankedTerms(new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/index__created_at_un_an/ranked_terms.ser"));
		TermInfo[] rankedTerms = new TermInfo[1];
		Term t = new Term("hash", "#egypt");
		TermInfo ti = new TermInfo(t, 10);
		rankedTerms[0] = ti;
		//graph the top terms by day
		trendList = getTrendedTerms(rankedTerms, new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/index__created_at_un_an/trended_terms.ser"));

		// need new class to do the graphing.
	}
	private static TermTrend[] getTrendedTerms(TermInfo[] rankedTerms, File inFile) throws Exception{
		
		TermTrend[] retVal = null;
		if(inFile.exists()){
			try{
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(inFile));
				retVal = (TermTrend[]) ois.readObject();
				ois.close(); 
			}
			catch(Exception e){}
		}
		else{
			TermDayTrend trends = new TermDayTrend();
			retVal = trends.getTrend(reader, rankedTerms);
			try {
				if(retVal != null){
				FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/index__created_at_un_an/trended_terms.ser");
				ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
				objectOut.flush();
				objectOut.writeObject(retVal);
				objectOut.close();
				}
			}
			catch(Exception e){
				if(inFile.exists()){
					inFile.delete();
				}
				e.printStackTrace();
			}
		}
		return retVal;	
	}
	
	private static TermInfo[] getRankedTerms(File inFile) throws Exception{
		TermInfo[] retVal = null;
		if(inFile.exists()){
			try{
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(inFile));
				retVal = (TermInfo[]) ois.readObject();
				ois.close(); 
			}
			catch(Exception e){}
		}
		else{
			retVal = termRank.getHighFreqTerms(reader, defaultNumTerms, field);
			try {
				if(retVal != null){
				FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/index__created_at_un_an/ranked_terms.ser");
				ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
				objectOut.flush();
				objectOut.writeObject(retVal);
				objectOut.close();
				}
			}
			catch(Exception e){
				if(inFile.exists()){
					inFile.delete();
				}
				e.printStackTrace();
			}
		}
		return retVal;		
	}
}