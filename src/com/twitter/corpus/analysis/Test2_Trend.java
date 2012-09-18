package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.Serialization2;

public class Test2_Trend {
	public static final Logger LOG = Logger.getLogger(Test2_Trend.class);
	public static void main(String args[]) throws IOException{
		testTrend();
		
		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/Test Trend/Round 2/list of terms.csv";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/Test Trend/Round 2/results.csv";
		TestOutput.printLinearTrend(input, output);
		
	}
	public static void testTrend() throws IOException{
		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/Test Trend/Round 2/";

		HashMap<String, Integer> stableTermsFreqs = new HashMap<String, Integer>();
		HashMap<Integer, Integer> tfMap = Serialization2.deserialize(input+"ds/tfMap.ser");
		HashBiMap<String, Integer> tbm = Serialization2.deserialize(input+"ds/trimTermBimapm0.05.ser");
		HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> jaccardNodes = Serialization2.deserialize(input+"tc_0.05/jnodes/jaccardAllNodes.ser");
		HashMap<Integer, HashMap<Integer, Double>>  jaccardLinear = Serialization2.deserialize(input + "jaccardNon_Weighted.ser");
		HashMap<Integer, Double> intervalTermCosetAvgSize = Serialization2.deserialize(input + "ds/IntervalCosetAvgSize.ser");

		double adjDiff = 0.3;

		double nodeTop = 0.7;
		double nodeBottom = 0.2;

		HashSet<Integer> closeTerms = new HashSet<Integer>(200);

		// iterate adjacent jaccard set first

		Iterator<Entry<Integer, HashMap<Integer, Double>>> jaccLinIter = jaccardLinear.entrySet().iterator();
		while(jaccLinIter.hasNext()){
			double linearCount = 0.0;
			Entry<Integer, HashMap<Integer, Double>> termEntry = jaccLinIter.next();

			Integer term = termEntry.getKey();

			HashMap<Integer, Double> termJaccLin = termEntry.getValue();

			Iterator<Entry<Integer, Double>> termJaccIter = termJaccLin.entrySet().iterator();

			double oldJ = 0.0;
			while(termJaccIter.hasNext()){
				Entry<Integer, Double> jacc = termJaccIter.next();
				double nextJ = jacc.getValue();

				if(Math.abs(nextJ - oldJ) > adjDiff){
					linearCount++;
				}
				oldJ = nextJ;
			}
			if(linearCount < 5){
				closeTerms.add(term);
			}
		}

		HashMap<String, Integer> outputList = new HashMap<String, Integer>(closeTerms.size());
		
		Iterator<Integer> termsIter = closeTerms.iterator();
		while(termsIter.hasNext()){
			int nodeCount = 0;
			double avgStability = 0.0;
			Integer t = termsIter.next();
			// outer i level, inner j level => iterate all
			if(jaccardNodes.get(t) != null){
				HashMap<Integer, HashMap<Integer, Double>> termJaccNodes = jaccardNodes.get(t);
				Iterator<Entry<Integer, HashMap<Integer, Double>>> iLevelIter = termJaccNodes.entrySet().iterator();
				while(iLevelIter.hasNext()){
					Entry<Integer, HashMap<Integer, Double>> entry = iLevelIter.next();
					Iterator<Entry<Integer, Double>> jLevelIter = entry.getValue().entrySet().iterator();
					while(jLevelIter.hasNext()){
						Entry<Integer, Double> jEntry = jLevelIter.next();

						double jacc = jEntry.getValue();
						avgStability += jacc;
						if(jacc < nodeTop && jacc > nodeBottom){
							nodeCount++;
						}
					}
				}
				if(nodeCount < 130){
					avgStability =  (double)Math.round(((double)avgStability/528) * 1000) / 1000;
					
					outputList.put(t + " " + tbm.inverse().get(t) + " "+ avgStability +" "+ intervalTermCosetAvgSize.get(t),tfMap.get(t));
				}
			}
		}

		TestOutput.printMap(outputList, output + " test 2.csv");

		//		TestOutput.printMap(stableTermsFreqs, output + "Avg stableMap.csv");
	}
}