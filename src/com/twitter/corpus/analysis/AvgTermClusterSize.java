package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.twitter.corpus.types.CoWeight;
import com.twitter.corpus.types.SerializationHelper;

/** 
 * Provide a printout of average term cluster size for graphing in Excel
 * Output: csv
 * @author dock
 *
 */
public class AvgTermClusterSize {
	private static final Logger LOG = Logger.getLogger(AvgTermClusterSize.class);
	public static void main(String args[]) throws IOException{
		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/tc/";
		String root = "termCoset_";
		String base = ".ser";
		
		String fileOut = "/home/dock/Documents/IR/AmazonResults/mRange3/ds/IntervalCosetAvgSize.ser";
		
		String[] filePaths = new String[33];
		for(int i = 0; i < 33 ; i++){
			filePaths[i] = input + root + (i+1) + base;
		}
		
		// get cosets
		LOG.info("getting cosets");
		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> cosetArray = new ArrayList<HashMap<Integer,ArrayList<CoWeight>>>(33);
		for(String path : filePaths){
			HashMap<Integer, ArrayList<CoWeight>> coset = SerializationHelper.deserialize(path);
			cosetArray.add(coset);
		}
		
		HashMap<Integer, Integer> retVal = new HashMap<Integer, Integer>((int)(cosetArray.get(0).size()*1.2));
		
		Iterator<HashMap<Integer, ArrayList<CoWeight>>> cosetIter = cosetArray.iterator();
		// iterate interval
		while(cosetIter.hasNext()){
			HashMap<Integer, ArrayList<CoWeight>> intervalEntry = cosetIter.next();
			
			// iterate terms
			Iterator<Entry<Integer, ArrayList<CoWeight>>> intervalEntryIter = intervalEntry.entrySet().iterator();
			while(intervalEntryIter.hasNext()){
				Entry<Integer, ArrayList<CoWeight>> termEntry = intervalEntryIter.next();
				
				Integer term = termEntry.getKey();
				int size = termEntry.getValue().size();
								
				if(!retVal.containsKey(term)){
					retVal.put(term, size);
				}
				else{
					int current = retVal.get(term);
					retVal.put(term, current + size);
				}
			}
		}
		
		HashMap<Integer, Double> retVal2 = new HashMap<Integer, Double>((int)(retVal.size()*1.2));
		Iterator<Entry<Integer, Integer>> retValIter = retVal.entrySet().iterator();
		while(retValIter.hasNext()){
			Entry<Integer, Integer> entry = retValIter.next();
			retVal2.put(entry.getKey(), (double)Math.round(((double)entry.getValue()/33) * 10) / 10);
		}
		
//		Serialization2.serialize(retVal2, fileOut);
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/AmazonResults/mRange3/ds/avgClusterSize.csv"));
		Iterator<Entry<Integer, Double>> avgSizeIter = retVal2.entrySet().iterator();
		
		while(avgSizeIter.hasNext()){
			Entry<Integer, Double> entryx = avgSizeIter.next();
			bw.append(entryx.getKey() + ","  + entryx.getValue()+"\n");
		}
		bw.close();
	}
}