package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
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
import com.twitter.corpus.types.ProcessedTweet;

public class InvertedIndex {
	public InvertedIndex(){
	}
	private static final Logger LOG = Logger.getLogger(InvertedIndex.class);
	public static int counter =1;
	//		private static int tfCount =1;

	/**
	 * @param StatusStream stream
	 * @return <del>HashMap<Integer, HashSet<Long>> InvertIndex</del> IndexAndDocCount
	 * <br><br>
	 * Returns the index built from a directory
	 * @throws IOException
	 */
	public HashMap<Integer, HashSet<Long>> buildIndex(StatusStream stream/*, int lowerCut, int upperCut*/) throws IOException{
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
//				if(docNum > 10000){
//					LOG.info(termIndex.size() + " total terms.");
//					break;
//				}
			}
			Long currTime = System.currentTimeMillis();
			LOG.info(docNum + " tweets indexed in " +  Admin.getTime(lastTime, currTime));
			lastTime = currTime;
		}
		finally
		{
		}
		HashMap<Integer, HashSet<Long>> thresholdIndex = new HashMap<Integer, HashSet<Long>>((int)termIndex.size()/2);

		Iterator<Map.Entry<Integer, HashSet<Long>>> indexIterator = termIndex.entrySet().iterator();
		while(indexIterator.hasNext()){
			Map.Entry<Integer, HashSet<Long>> termEntry = indexIterator.next();

			// frequency threshold. There is a global and local consideration. Thresholds are calculated with Global values
			// However for the purpose of index sizes, an threshold of tf < 2 say will be applied
			if(termEntry.getValue().size() > TweetAnalysis.lowCutoffGlobal /*lowerCut && termEntry.getValue().size() < upperCut*/){
				thresholdIndex.put(termEntry.getKey(), termEntry.getValue());
			}
		}
		LOG.info("Trimmed interval index using tf <  " + TweetAnalysis.lowCutoffGlobal + " size is: " + thresholdIndex.size());

//		ArrayList<Integer> termArrayList = new ArrayList<Integer>();
//		Integer cutPoint = 7300;
//		termArrayList.ensureCapacity(cutPoint);
//
//		int cutCounter = 0;
//		Iterator<Map.Entry<Integer, HashSet<Long>>> indexIter = termIndex.entrySet().iterator();
//		while(indexIter.hasNext() && cutCounter < cutPoint){			
//			Map.Entry<Integer, HashSet<Long>> termEntry = indexIter.next();
//			Integer term = termEntry.getKey();
//			termArrayList.add(term);
//			cutCounter++;
//		}		
		return thresholdIndex;
	}
	/**
	 * Trim tf terms from the local index using Global docCount values after local index is added to global corpus index.
	 * I.E. tf should be greater than 33*cutpoint
	 * @param intervalTermIndex
	 * @param lower
	 * @param upper
	 * @return trimmed index
	 */
	public static ArrayList<HashMap<Integer, HashSet<Long>>> trimLocalIndices(ArrayList<HashMap<Integer, HashSet<Long>>> intervalTermIndex, int lower, int upper){
		ArrayList<HashMap<Integer, HashSet<Long>>> retVal = new ArrayList<HashMap<Integer,HashSet<Long>>>(intervalTermIndex.size());
		
		Iterator<HashMap<Integer, HashSet<Long>>> indexIter = intervalTermIndex.iterator();
		
		while(indexIter.hasNext()){
			HashMap<Integer, HashSet<Long>> localIndex = indexIter.next();
			
			Iterator<Map.Entry<Integer, HashSet<Long>>> localIndexIter = localIndex.entrySet().iterator();
			HashMap<Integer, HashSet<Long>> trimmedIndex = new HashMap<Integer, HashSet<Long>>((int)localIndex.size()/2);
			while(localIndexIter.hasNext()){
				Map.Entry<Integer, HashSet<Long>> entry = localIndexIter.next();
				Integer term = entry.getKey();
				HashSet<Long> docSet = entry.getValue();

				// check against GLOBAL values
				if(!(TweetAnalysis.corpusIndex.get(term).size() < lower || TweetAnalysis.corpusIndex.get(term).size() > upper)){
					trimmedIndex.put(term, docSet);
				}
			}
			retVal.add(trimmedIndex);
		}
		return retVal;
	}

	/**
	 * merge local index with global index, iterate local index and merge existing document sets for a particular term, or add new terms with document set
	 * @param intervalTermIndex
	 */
	public static void mergeLocalIndex(HashMap<Integer, HashSet<Long>> intervalTermIndex){
		Iterator<Map.Entry<Integer, HashSet<Long>>> intervalIndexIter = intervalTermIndex.entrySet().iterator();
		while(intervalIndexIter.hasNext()){
			Map.Entry<Integer, HashSet<Long>> entry = intervalIndexIter.next();
			Integer term = entry.getKey();
			HashSet<Long> docs = entry.getValue();

			if(TweetAnalysis.corpusIndex.containsKey(term)){
				TweetAnalysis.corpusIndex.get(term).addAll(docs);
			}
			else{
				TweetAnalysis.corpusIndex.put(term, docs);
			}
		}
	}
	public static void globalIndexSerialize(HashMap<Integer, HashSet<Long>> globalTermIndex, String outputPath) throws IOException{
		LOG.info("Serializing Global Index.");
		String path = outputPath + "/globalIndex.ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(globalTermIndex);
		objectOut.close();
		LOG.info("Finished serializing global Index.");
	}
	public static void localIndexArraySerialize(ArrayList<HashMap<Integer, HashSet<Long>>> localTermIndex, String outputPath) throws IOException{
		LOG.info("Serializing local Index array.");
		String path = outputPath + "/LocalIndexArray.ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(localTermIndex);
		objectOut.close();
		LOG.info("Finished serializing local Index.");
	}
	public static int getDocCount(HashMap<Integer, HashSet<Long>> corpusIndex){
		int retVal = 0;
		Iterator<Map.Entry<Integer, HashSet<Long>>> indexIter = corpusIndex.entrySet().iterator();
		while(indexIter.hasNext()){
			Entry<Integer, HashSet<Long>> entry = indexIter.next();
			retVal += entry.getValue().size();
		}
		return retVal;
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
			if(entry.getValue().size() > 2){
				freqArrayL.add(entry);
			}
		}

		Collections.sort(freqArrayL, new indexComparator());

		Iterator<Map.Entry<Integer, HashSet<Long>>> arrayIter = freqArrayL.iterator();

		while(arrayIter.hasNext()){
			Map.Entry<Integer, HashSet<Long>> entry = arrayIter.next();
			bf.append(String.valueOf(entry.getValue().size()));
			bf.append("\n");
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