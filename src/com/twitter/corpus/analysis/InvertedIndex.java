package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;
import com.twitter.corpus.demo.Admin;
import com.twitter.corpus.demo.ProcessedTweet;
import com.twitter.corpus.demo.TweetProcessor;

public class InvertedIndex {
	public InvertedIndex(){
	}
	private static final Logger LOG = Logger.getLogger(TermTermWeights.class);
	//	public static int counter =1;

	/**
	 * 
	 * @param StatusStream stream
	 * @return <del>HashMap<Integer, HashSet<Long>> InvertIndex</del> IndexAndDocCount
	 * <br><br>
	 * Returns the index built from a directory
	 * @throws IOException
	 */
	public HashMap<Integer, HashSet<Long>> buildIndex(StatusStream stream) throws IOException{
		int skip=0;
		if(TweetProcessor.stopwords == null){
			TweetProcessor.callStops();
		}
		// Integer -> term id, hashset of long -> doc occurances

		HashMap<Integer, HashSet<Long>> termIndex = new HashMap<Integer, HashSet<Long>>();

		int docNum=0;
		Status status;
		Long lastTime = 0l;
		try {
			while ((status = stream.next()) != null)
			{
				//				skip++;
				//				if(skip!=17){
				//					continue;
				//				}
				//				if(skip==17){skip =0;}
				// status 302 is a redirect, ie a retweet
				if(status.getHttpStatusCode() == 302){
					continue;
				}					

				String tweet = status.getText();
				if (tweet == null){	continue;}
				ProcessedTweet pt = TweetProcessor.processTweet(status.getText(),status.getId());

				for(int i=0; i< pt.termIdList.size() ; i++){
					if(!termIndex.containsKey(pt.termIdList.get(i)))
					{// tdh - termDocHash
						HashSet<Long> tdh = new HashSet<Long>();
						if(!tdh.contains(status.getId())){
							tdh.add(status.getId());
						}
						termIndex.put(pt.termIdList.get(i), tdh);
					}
					else
					{
						if(!termIndex.get(pt.termIdList.get(i)).contains(status.getId())){
							termIndex.get(pt.termIdList.get(i)).add(status.getId());
						}
					}
				}
				docNum++;
				if(docNum % 10000 == 0 ){
					Long currTime = System.currentTimeMillis();
					LOG.info(/*"block: "+counter+"*/ docNum + " tweets processed in " +  Admin.getTime(lastTime, currTime));
					lastTime = currTime;
				}
				if(docNum > 100000){
					LOG.info(termIndex.size() + " total terms.");
					//					counter++;
					break;
				}
			}
		}
		finally
		{
		}
//		return new IndexAndDocCount(docNum, termIndex);
				return termIndex;
	}
	public HashMap<Integer, Double> getTfidf(HashMap<Integer, HashSet<Long>> index) throws IOException{
		HashMap<Integer, Double> tfidfMap = new HashMap<Integer, Double>(index.size());
		Iterator<Entry<Integer, HashSet<Long>>> indexIterator = index.entrySet().iterator();

		// calcualte idf values
		while(indexIterator.hasNext()){
			Entry<Integer, HashSet<Long>> term = indexIterator.next();
			// get IDF portion, ie terms relevance across corpus
			Double tfidf = Math.log10(((double)TermTermWeights.docTermsMap.size()/(double)index.get(term.getKey()).size()));
			Double roundTfIdf = (double)Math.round(tfidf * 1000) / 1000;
			tfidfMap.put(term.getKey(), roundTfIdf);
		}
		HashMap<Double, Integer> freqs = new HashMap<Double, Integer>();
		
		// count up all idf values
		Iterator<Map.Entry<Integer, Double>> iter = tfidfMap.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<Integer, Double> entry = iter.next();

			if(!freqs.containsKey(entry.getValue())){
				freqs.put(entry.getValue(), 1);
			}
			else{
				freqs.put(entry.getValue(),freqs.get(entry.getValue())+1);
			}
		}
		
		// sort idf values descending and put in array..
		ArrayList<Map.Entry<Double,Integer>> sortedArray = new ArrayList<Map.Entry<Double,Integer>>(freqs.size());
		Iterator<Map.Entry<Double, Integer>> qwe = freqs.entrySet().iterator();
		while(qwe.hasNext()){
			Map.Entry<Double,Integer> next = qwe.next();
			sortedArray.add(next);					
		}
		Collections.sort(sortedArray, new idfComparator());
				
				
		BufferedWriter br = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/idf.txt"));
		Iterator<Map.Entry<Double,Integer>> it = freqs.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Double,Integer> ent = it.next();
			br.append(ent.getKey() + "\t" + ent.getValue() + "\n");
		}
		br.close();
		return tfidfMap;
	}
	public static class idfComparator implements Comparator<Map.Entry<Double,Integer>> {
		@Override
		public int compare(Map.Entry<Double,Integer> arg0, Map.Entry<Double,Integer> arg1) {
			return arg0.getValue().compareTo(arg1.getValue());
		}
	}
}

























