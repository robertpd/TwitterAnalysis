package com.twitter.corpus.translation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.CoWeight;
import com.twitter.corpus.types.SerializationHelper;

/**
 * Provide a translation of coset terms from their integer rep to string values. 
 * This performed for each interval of analysis. Interval ~ day
 * @author dock
 *
 */
public class DecodeTermCosetsForInterval {
	private static final Logger LOG = Logger.getLogger(DecodeTermCosetsForInterval.class);
	public static void main(String[] args) throws IOException{
		
		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/tc/";
		String root = "termCoset_";
		String base = ".ser";
		
		String fileOut = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/termCosetsStrings.txt";
		String intputTerms = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/Test Stable/Round 2 added avg cluster size/0.7 Avg stableMap (copy).csv";
		
		String termBimapPath = "/home/dock/Documents/IR/AmazonResults/mRange3/ds/trimTermBimapm0.05.ser";
		String tfMapPath = "/home/dock/Documents/IR/AmazonResults/mRange3/ds/tfMap.ser";
		
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
		LOG.info("Deser termBimap");
		HashBiMap<String, Integer> termBimap = SerializationHelper.deserialize(termBimapPath);
		HashMap<Integer, Integer> tfMap = SerializationHelper.deserialize(tfMapPath);
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(fileOut));
		
		LOG.info("Output translations for cosets");
		HashSet<Integer> termSet =  (HashSet<Integer>) SerializationHelper.readFile(intputTerms);
		Iterator<Integer> termSetI = termSet.iterator();
		
		//generate the csv file, outputting term integer, term string
		while(termSetI.hasNext()){
			Integer entry = termSetI.next();
			bw.append("\nTerm: " + termBimap.inverse().get(entry).toString() + " (t_id:" + entry + ", daily tf:" + (tfMap.get(entry)/33) + ")\n");
			for(int i =0; i < cosetArray.size(); i++){
				HashMap<Integer, ArrayList<CoWeight>> cosetEntry = cosetArray.get(i);
				if(cosetEntry.containsKey(entry)){
					bw.append(i + " ");

					Iterator<CoWeight> coweightIter = cosetEntry.get(entry).iterator();
					while(coweightIter.hasNext()){
						CoWeight termEntry = coweightIter.next();
						if(termEntry.correlate >= 0.05){
							bw.append(termBimap.inverse().get(termEntry.termId));
							bw.append(", ");
						}
					}
					bw.append("\n");bw.flush();
				}
				else{ bw.append(i + "\n");}
			}
		}
		bw.close();
	}
}