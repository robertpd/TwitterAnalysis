package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.Serialization2;

public class Test1_Stable {

	public static void main(String args[]) throws IOException{
		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/";
		double stableThreshold = 0.9;
		
		HashMap<String, Integer> stableTermsFreqs = new HashMap<String, Integer>();
		
		HashMap<Integer, Integer> tfMap = Serialization2.deserialize(input+"ds/tfMap.ser");
		
		HashBiMap<String, Integer> tbm = Serialization2.deserialize(input+"ds/trimTermBimapm0.05.ser");
		
		HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> jaccardNodes = Serialization2.deserialize(input+"tc_0.05/jnodes/jaccardAllNodes.ser");
		Iterator<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> jNodesIter = jaccardNodes.entrySet().iterator();
		// iterate terms
		while(jNodesIter.hasNext()){
			boolean pass = true;
			Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>> jNodesEntry = jNodesIter.next();
			int term = jNodesEntry.getKey();
			HashMap<Integer, HashMap<Integer, Double>> termJNodes = jNodesEntry.getValue();
			
			Iterator<Entry<Integer, HashMap<Integer, Double>>> termJnodesIter = termJNodes.entrySet().iterator();
			//iterate i level for term
			while(termJnodesIter.hasNext()){
				Entry<Integer, HashMap<Integer, Double>> iLevelEntry = termJnodesIter.next();						
				int i = iLevelEntry.getKey();
				HashMap<Integer, Double> iValue = iLevelEntry.getValue();
				
				Iterator<Entry<Integer, Double>> iValueIter = iValue.entrySet().iterator();
				// iterate each jaccard value
				while(iValueIter.hasNext()){
					double jac = iValueIter.next().getValue();
					if(jac < stableThreshold){
						pass = false;
						break;
					}
				}
				if(pass){stableTermsFreqs.put(term + " " + tbm.inverse().get(term), tfMap.get(term));}
			}
		}
		TestOutput.printMap(stableTermsFreqs, output+"stableMap.csv");
	}
}
