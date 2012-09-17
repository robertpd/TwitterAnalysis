package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.Serialization2;

public class Test1_Stable {
	public static final Logger LOG = Logger.getLogger(Test1_Stable.class);
	public static void main(String args[]) throws IOException{
		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/";
		double stableThreshold = 0.7;

		HashMap<String, Integer> stableTermsFreqs = new HashMap<String, Integer>();

		HashMap<Integer, Integer> tfMap = Serialization2.deserialize(input+"ds/tfMap.ser");

		HashBiMap<String, Integer> tbm = Serialization2.deserialize(input+"ds/trimTermBimapm0.05.ser");

		HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> jaccardNodes = Serialization2.deserialize(input+"tc_0.05/jnodes/jaccardAllNodes.ser");
		Iterator<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> jNodesIter = jaccardNodes.entrySet().iterator();
		// iterate terms
		while(jNodesIter.hasNext()){
			boolean pass = true;
			double avgStability = 0.0;
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
					avgStability += jac;
					if(jac < stableThreshold){
						pass = false;
						break;
					}
				}
			}
			if(pass){
				stableTermsFreqs.put(term + " " + tbm.inverse().get(term) + " " + avgStability/528, tfMap.get(term));
			}
		}
		TestOutput.printMap(stableTermsFreqs, output + stableThreshold + " Avg stableMap.csv");
		LOG.info("Finished Test: Stable terms with threshold: " + stableTermsFreqs);
	}
}
