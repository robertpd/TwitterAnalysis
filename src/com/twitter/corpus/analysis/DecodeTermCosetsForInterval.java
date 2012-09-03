package com.twitter.corpus.analysis;

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
import com.twitter.corpus.types.Serialization2;

public class DecodeTermCosetsForInterval {
	private static final Logger LOG = Logger.getLogger(DecodeTermCosetsForInterval.class);
	public static void main(String[] args) throws IOException{
		
		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/tc/";
		String root = "termCoset_";
		String base = ".ser";
		
		String fileOut = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/528/termCosetsStrings.txt";
		String intputTerms = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/528/0.3 terms.csv";
		
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
			HashMap<Integer, ArrayList<CoWeight>> coset = Serialization2.deserialize(path);
			cosetArray.add(coset);
		}
		LOG.info("Deser termBimap");
//		HashBiMap<String, Integer> termBimap = deserTermBiMap(termBimapPath);
		HashBiMap<String, Integer> termBimap = Serialization2.deserialize(termBimapPath);
		HashMap<Integer, Integer> tfMap = Serialization2.deserialize(tfMapPath);
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(fileOut));
		
		LOG.info("Output translations for cosets");
		HashSet<Integer> termSet =  (HashSet<Integer>) Serialization2.readFile(intputTerms);
		Iterator<Integer> termSetI = termSet.iterator();
		while(termSetI.hasNext()){
			Integer entry = termSetI.next();
			for(int i =0; i < cosetArray.size(); i++){
				HashMap<Integer, ArrayList<CoWeight>> cosetEntry = cosetArray.get(i);
				if(cosetEntry.containsKey(entry)){
					bw.append(i + " " + termBimap.inverse().get(entry).toString() + " (" + tfMap.get(entry) + ") : ");
					
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
				else{
					bw.append(i + "\n");
				}
			}
		}
		bw.close();
	}
}