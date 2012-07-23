package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
		public static int counter =1;
		private static int tfCount =1;
	/**
	 * @param StatusStream stream
	 * @return <del>HashMap<Integer, HashSet<Long>> InvertIndex</del> IndexAndDocCount
	 * <br><br>
	 * Returns the index built from a directory
	 * @throws IOException
	 */
	public HashMap<Integer, HashSet<Long>> buildIndex(StatusStream stream, int lowerCut, int upperCut) throws IOException{
		if(TweetProcessor.stopwords == null){
			TweetProcessor.callStops();
		}
		// Integer -> term id, hashset of long -> doc occurances
		HashMap<Integer, HashSet<Long>> termIndex = new HashMap<Integer, HashSet<Long>>();

		int docNum=0;
		Status status;
		Long lastTime = System.currentTimeMillis();
		try {
			while ((status = stream.next()) != null)
			{
				// status 302 is a redirect, ie a retweet
				if(status.getHttpStatusCode() == 302){
					continue;
				}

				String tweet = status.getText();
				if (tweet == null){	continue;}
				ProcessedTweet pt = TweetProcessor.processTweet(status.getText(),status.getId());
				// blacnked for termbimap
				for(int i=0; i< pt.termIdList.size() ; i++){
					if(!termIndex.containsKey(pt.termIdList.get(i)))
					{// tdh - termDocHash
						HashSet<Long> termDocHashSet = new HashSet<Long>(15);
						if(!termDocHashSet.contains(status.getId())){
							termDocHashSet.add(status.getId());
						}
						termIndex.put(pt.termIdList.get(i), termDocHashSet);
					}
					else
					{
						if(!termIndex.get(pt.termIdList.get(i)).contains(status.getId())){
							termIndex.get(pt.termIdList.get(i)).add(status.getId());
						}
					}
				}
				docNum++;
				if(docNum % 200000 == 0 ){
					Long currTime = System.currentTimeMillis();
					LOG.info(docNum + " tweets indexed in " +  Admin.getTime(lastTime, currTime));
					lastTime = currTime;
				}
				
//				if(docNum > 50000){
//					LOG.info(termIndex.size() + " total terms.");
//					break;
//				}
			}
			LOG.info(termIndex.size() + " total terms indexed. Doc count >= 0.");
		}
		finally
		{
		}
		
		// remove index terms with df less than a certain amount and > a certain amount
		// TODO Sort out tf upper threshold. Investigate weighting, currently looking at raw term frequencies over daily corpus, need refined weighting
		HashMap<Integer, HashSet<Long>> thresholdIndex = new HashMap<Integer, HashSet<Long>>((int)termIndex.size()/2);
//		
		Iterator<Map.Entry<Integer, HashSet<Long>>> indexIterator = termIndex.entrySet().iterator();
		while(indexIterator.hasNext()){
			Map.Entry<Integer, HashSet<Long>> termEntry = indexIterator.next();
			// frequency threshold
			if(termEntry.getValue().size() > lowerCut && termEntry.getValue().size() < upperCut){
				thresholdIndex.put(termEntry.getKey(), termEntry.getValue());
			}
		}
		LOG.info(thresholdIndex.size() + " term after trimming.");
		
		// add TF-IDF stopword removal by taking occurance of term over 1000 docs. In a day there are avg 730k docs
		
		// iterate and take a term
		// iterate 1000 docs and count occurances
		// [ occurances / docs * term count of each doc ] * log[ total Docs / docs ]
		// store this in a map for inspection and to determine cut point
		// remove terms that correspond to this cut point
		
		// the first time the term occurs take 1000 docs following that .
		// first time 
		
		// take 7300 docs.
		// add them to arraylist to calc total size termCount
		// after building arraylist, use to create set for fast checking
		
		ArrayList<Integer> termArrayList = new ArrayList<Integer>();
		Integer cutPoint = 7300;
		termArrayList.ensureCapacity(cutPoint);
		
		// populate window
		
		int cutCounter = 0;
		Iterator<Map.Entry<Integer, HashSet<Long>>> indexIter = termIndex.entrySet().iterator();
		while(indexIter.hasNext() && cutCounter < cutPoint){			
			Map.Entry<Integer, HashSet<Long>> termEntry = indexIter.next();
			Integer term = termEntry.getKey();
			termArrayList.add(term);
			cutCounter++;
		}
		int arraySize = termArrayList.size();
		
		// init TFIDF mapping		
		HashMap<Integer, Double> termTFIDF = new HashMap<Integer, Double>(termIndex.size());
		
		// calc TFIDF's
		Iterator<Map.Entry<Integer, HashSet<Long>>> termIter2 = termIndex.entrySet().iterator();
		while(termIter2.hasNext()){
			Entry<Integer, HashSet<Long>> entry = termIter2.next();
			Integer term = entry.getKey();
			
			int tf=0;
			Iterator<Integer> arrayListIter = termArrayList.iterator();
			while(arrayListIter.hasNext()){
				if(arrayListIter.next() == term){
					tf++;
				}
			}
			Double tfidf = ((double)tf / (double)arraySize ) * (Math.log(termIndex.size() /termIndex.get(term).size()));
			tfidf = (double)Math.round(tfidf * 10000000) / 10000000;
			termTFIDF.put(term, tfidf);
		}

		// tfidf term print
		
		//		BufferedWriter bf = new BufferedWriter(new FileWriter("/analysis/output/" + tfCount + "_tfidf.txt"));
//		Iterator<Map.Entry<Integer, Double>> tfIter = termTFIDF.entrySet().iterator();
//		while(tfIter.hasNext()){
//			Entry<Integer, Double> ent = tfIter.next();
//			bf.append(ent.getKey() + ", " + ent.getValue() + "\n");
//		}
//		bf.close();
		tfCount++;
		return thresholdIndex;
	}
	
	public static void indexSerialize(HashMap<Integer, HashSet<Long>> termIndex, String outputPath) throws IOException{
		LOG.info("Serializing Index.");
		String path = outputPath + "/index.ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(termIndex);
		objectOut.close();
		LOG.info("Finished serializing Index.");
	}
	
	/**
	 * Get the tfIdf for each term
	 * @param index
	 * @return a mapping of terms and their tf's
	 * @throws IOException
	 */
	public HashMap<Integer, Double> getTfidf(HashMap<Integer, HashSet<Long>> index) throws IOException{
		HashMap<Integer, Double> tfidfMap = new HashMap<Integer, Double>(index.size());
		ArrayList<Double> tfidfArrayList = new ArrayList<Double>(index.size());

		Iterator<Entry<Integer, HashSet<Long>>> index2TFIterator = index.entrySet().iterator();
		int http=0;
		while(index2TFIterator.hasNext()){
			Entry<Integer, HashSet<Long>> term = index2TFIterator.next();
			if(TermTermWeights.termBimap.inverse().get(term.getKey()).contains("http")){
				http++;
			}
		}
		
		Iterator<Entry<Integer, HashSet<Long>>> indexTFIterator = index.entrySet().iterator();

		// print out  list of the low freq terms ie tf=1,2,3,4...
		BufferedWriter lowTfPtint = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/"+counter+" lowTf_.txt"));
		BufferedWriter highTfPtint = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/"+counter+" highTf.txt"));
		ArrayList<tfPair> tf = new ArrayList<tfPair>(index.size());
		BufferedWriter idfPrint = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/"+counter+" idf.txt"));
		int lowLineCnt=0;
		int highLineCnt = 0;
		while(indexTFIterator.hasNext()){
			Entry<Integer, HashSet<Long>> term = indexTFIterator.next();
			// tf lower threshold
			if(term.getValue().size() < 16){
				lowTfPtint.append(TermTermWeights.termBimap.inverse().get(term.getKey()) + ": " + term.getValue().size() + ", ");
				lowLineCnt++;
				if(lowLineCnt ==10){
					lowLineCnt = 0;
					lowTfPtint.append("\n");
				}
			}
			
			// tf upper threshold
			if(term.getValue().size() >500){
				highTfPtint.append(TermTermWeights.termBimap.inverse().get(term.getKey()) + ": " + term.getValue().size() + ", ");
				highLineCnt++;
				if(highLineCnt ==10){
					highLineCnt = 0;
					highTfPtint.append("\n");
				}
			}
			tf.add(new tfPair(term.getKey(), term.getValue().size()));
		}
		lowTfPtint.close();
		highTfPtint.close();
		
		// deal with full tf 
		Collections.sort(tf, new tfComparator2());
		Iterator<tfPair> tfIter = tf.iterator();
		while(tfIter.hasNext()){
			tfPair tfp = tfIter.next();
			idfPrint.append(TermTermWeights.termBimap.inverse().get(tfp.term) + ", " + tfp.tf.toString() + "\n");
		}
		idfPrint.close();
		counter++;
		return tfidfMap;
	}
	public static void printFrequencies(HashMap<Integer, HashSet<Long>> index, String path) throws IOException{
		BufferedWriter bf = new BufferedWriter(new FileWriter(path));
		
		ArrayList<Map.Entry<Integer, HashSet<Long>>> freqArrayL = new ArrayList<Map.Entry<Integer,HashSet<Long>>>(index.size());
		
		Iterator<Map.Entry<Integer, HashSet<Long>>> indexIter = index.entrySet().iterator();
		while(indexIter.hasNext()){
			Entry<Integer, HashSet<Long>> entry = indexIter.next();
			freqArrayL.add(entry);			
		}
		
		Collections.sort(freqArrayL, new indexComparator());
		
		Iterator<Map.Entry<Integer, HashSet<Long>>> arrayIter = freqArrayL.iterator();
		
		while(arrayIter.hasNext()){
			Map.Entry<Integer, HashSet<Long>> entry = arrayIter.next();
			bf.append(entry.getValue().size() + "\n");
		}
		bf.close();
	}
	public static class indexComparator implements Comparator<Map.Entry<Integer, HashSet<Long>>> {

		@Override
		public int compare(Map.Entry<Integer, HashSet<Long>> f1, Map.Entry<Integer, HashSet<Long>> f2) {

			if (f1.getValue().size() > f2.getValue().size()){
				return -1;
			}else if(f1.getValue().size() < f2.getValue().size()){
				return 1;
			}
			else
				return 0;
		}	
	}
	
	public class tfPair{
		public tfPair(Integer term, Integer tf){
			this.term = term;
			this.tf = tf;
		}
		public Integer term;
		public Integer tf;
	}

	public static class idfComparator implements Comparator<Double> {
		@Override
		public int compare(Double arg0, Double arg1) {
			// -1 to reverese the list, ie descending
			return (-1)*arg0.compareTo(arg1);
		}
	}
	public static class tfComparator implements Comparator<Integer> {
		@Override
		public int compare(Integer arg0, Integer arg1) {
			// -1 to reverese the list, ie descending
			return (-1)*arg0.compareTo(arg1);
		}
	}
	public static class tfComparator2 implements Comparator<tfPair> {
		@Override
		public int compare(tfPair arg0, tfPair arg1) {
			// -1 to reverese the list, ie descending
			return (-1)*arg0.tf.compareTo(arg1.tf);
		}
	}
}