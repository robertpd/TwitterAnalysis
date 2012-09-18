package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.Serialization2;

public class Test1_Stable {
	public static final Logger LOG = Logger.getLogger(Test1_Stable.class);
	public static void main(String args[]) throws IOException{
//		primaryAnalysis();
		String file = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/Test Trend/Round 2/list of terms.csv";
		printLinearTrend(file);
	}
	public static void primaryAnalysis() throws IOException{
		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/";
		double stableThreshold = 0.7;

		HashMap<String, Integer> stableTermsFreqs = new HashMap<String, Integer>();
		HashMap<Integer, Integer> tfMap = Serialization2.deserialize(input+"ds/tfMap.ser");
		HashBiMap<String, Integer> tbm = Serialization2.deserialize(input+"ds/trimTermBimapm0.05.ser");
		HashMap<Integer, Double> intervalCosetAvgTermSetSize = Serialization2.deserialize(input + "ds/IntervalCosetAvgSize.ser");

		HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> jaccardNodes = Serialization2.deserialize(input+"tc_0.05/jnodes/jaccardAllNodes.ser");
		Iterator<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> jNodesIter = jaccardNodes.entrySet().iterator();
		// iterate terms
		while(jNodesIter.hasNext()){
			boolean pass = true;
			double avgStability = 0.0;
			int missCount = 0;
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
						missCount++;
						if(missCount > 100){
							pass = false;
							break;
						}
					}
				}
			}
			if(pass){
				double avgStab = (double)Math.round(((double)avgStability/528) * 1000) / 1000;
				stableTermsFreqs.put(term + " " + tbm.inverse().get(term) + " " + avgStab + " " + intervalCosetAvgTermSetSize.get(term), tfMap.get(term));
			}
		}
		TestOutput.printMap(stableTermsFreqs, output + stableThreshold + " Avg stableMap.csv");
		LOG.info("Finished Test: Stable terms with threshold: " + stableTermsFreqs);
	}
	
	public static void printLinearTrend(String inputTerms) throws IOException{
		String base = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes";
//		String inputTerms = base + "/Test Stable/Round 2 added avg cluster size/10 miss terms.csv";
		String jaccardLinear = "/home/dock/Documents/IR/AmazonResults/mRange3/jaccardNon_Weighted.ser";
//		String outputCsv = base + "/Test Stable/Round 2 added avg cluster size/linearJaccardListForTerms.csv";
		String outputCsv = base + "/Test Trend/Round 2/linearJaccardListForTerms.csv";
		String termBimap = "/home/dock/Documents/IR/AmazonResults/mRange3/ds/trimTermBimapm0.05.ser";
		
		HashSet<Integer> termSet =  (HashSet<Integer>) Serialization2.readFile(inputTerms);
		HashMap<Integer, HashMap<Integer, Double>> jlin = Serialization2.deserialize(jaccardLinear);
		HashBiMap<String, Integer> bimap = Serialization2.deserialize(termBimap);
		HashMap<Integer, Double> intervalCosetAvgTermSetSize = Serialization2.deserialize("/home/dock/Documents/IR/AmazonResults/mRange3/ds/IntervalCosetAvgSize.ser");
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputCsv));
		
		Iterator<Integer> termSetIter = termSet.iterator();
		while(termSetIter.hasNext()){
			Integer term = termSetIter.next();
			bw.append(bimap.inverse().get(term));
			bw.append(",CS= " + intervalCosetAvgTermSetSize.get(term)+",");
			HashMap<Integer, Double> jaccLin = jlin.get(term);
			
			Iterator<Entry<Integer, Double>> jaccLinIter = jaccLin.entrySet().iterator();
			while(jaccLinIter.hasNext()){
				Entry<Integer, Double> jacEntry = jaccLinIter.next();
				bw.append(jacEntry.getValue().toString());
				bw.append(",");			
			}
			bw.append("\n");
		}
		bw.close();
	}
}