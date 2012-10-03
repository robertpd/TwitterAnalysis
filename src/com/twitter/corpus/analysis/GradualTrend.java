package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.Serialization2;

public class GradualTrend {
	public static final Logger LOG = Logger.getLogger(GradualTrend.class);
	static HashMap<Integer, Double> intervalTermCosetAvgSize;
	static HashBiMap<String, Integer> tbm;
	static HashMap<Integer, Integer> tfMap;
	public static void main(String args[]) throws IOException{
//		testTrend();

				String input = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/Test Trend/Round 2/termlistnew.csv";
				String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/Test Trend/Round 2/results2.csv";
				TestOutput.printLinearTrend(input, output);

	}
	public static void testTrend() throws IOException{
		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/Test Trend/Round 2/";

		tfMap = Serialization2.deserialize(input+"ds/tfMap.ser");
		tbm = Serialization2.deserialize(input+"ds/trimTermBimapm0.05.ser");
		HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> jaccardNodes = Serialization2.deserialize(input+"tc_0.05/jnodes/jaccardAllNodes.ser");
		HashMap<Integer, HashMap<Integer, Double>>  jaccardLinear = Serialization2.deserialize(input + "jaccardNon_Weighted.ser");
		intervalTermCosetAvgSize = Serialization2.deserialize(input + "ds/IntervalCosetAvgSize.ser");

		double adjDiff = 0.15;

		// iterate adjacent jaccard set first
		HashSet<Integer> closeTermSet = closeTerms(jaccardLinear,adjDiff);

		// check jaccard nodes for a difference in first 1/4 and last 1/4
		HashMap<String, Integer> outputList = getFinalSet(closeTermSet, jaccardNodes );

		TestOutput.printMap(outputList, output + " test new.csv");

		//		TestOutput.printMap(stableTermsFreqs, output + "Avg stableMap.csv");
	}
	public static HashSet<Integer> closeTerms(HashMap<Integer, HashMap<Integer, Double>>  jaccardLinear, double adjDiff)	{
		HashSet<Integer> retVal = new HashSet<Integer>();
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
					linearCount++;	// increment if diff is greater than allowable
				}
				oldJ = nextJ;
			}	
			if(linearCount < 5){	// only allow certain number of blips
				retVal.add(term);
			}
		}
		return retVal;
	}
	public static HashMap<String, Integer> getFinalSet(HashSet<Integer> closeTermSet, HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> jaccardNodes){
		HashMap<String, Integer> retVal = new HashMap<String, Integer>();

		Iterator<Integer> termsIter = closeTermSet.iterator();
		while(termsIter.hasNext()){
			int nodeCount = 0;
			double avgStability = 0.0;
			Integer t = termsIter.next();
			int startToFinishDifference=0;

			if(jaccardNodes.get(t) != null){
				HashMap<Integer, HashMap<Integer, Double>> termJaccNodes = jaccardNodes.get(t);

				for(int i = 23; i < 32; i++){
					for (int j=i ;j < 32; j++){
						double jaccsie = termJaccNodes.get(i).get(j);
						if(jaccsie < 0.6){
							startToFinishDifference++;
						}
					}
				}
			}


			if(startToFinishDifference > 10){
				avgStability =  (double)Math.round(((double)avgStability/528) * 1000) / 1000;

				retVal.put(t + " " + tbm.inverse().get(t) + " "+ avgStability +" "+ intervalTermCosetAvgSize.get(t),tfMap.get(t));
			}

		}

		return retVal;				
	}
	
	// old code
//	Iterator<Integer> termsIter = closeTermSet.iterator();
//	while(termsIter.hasNext()){
//		int nodeCount = 0;
//		double avgStability = 0.0;
//		Integer t = termsIter.next();
//		// outer i level, inner j level => iterate all
//		if(jaccardNodes.get(t) != null){
//			HashMap<Integer, HashMap<Integer, Double>> termJaccNodes = jaccardNodes.get(t);
//			Iterator<Entry<Integer, HashMap<Integer, Double>>> iLevelIter = termJaccNodes.entrySet().iterator();
//			while(iLevelIter.hasNext()){
//				Entry<Integer, HashMap<Integer, Double>> entry = iLevelIter.next();
//				Iterator<Entry<Integer, Double>> jLevelIter = entry.getValue().entrySet().iterator();
//				while(jLevelIter.hasNext()){
//					Entry<Integer, Double> jEntry = jLevelIter.next();
//
//					double jacc = jEntry.getValue();
//					avgStability += jacc;
//					//						if(jacc < nodeTop && jacc > nodeBottom && jacc != 1.0) {
//					//							nodeCount++;
//					//						}
//					if(jacc > nodeBottom){
//						nodeCount++;
//					}
//				}
//			}
//			//				if(nodeCount < 130){
//			if(nodeCount > 180){
//				avgStability =  (double)Math.round(((double)avgStability/528) * 1000) / 1000;
//
//				outputList.put(t + " " + tbm.inverse().get(t) + " "+ avgStability +" "+ intervalTermCosetAvgSize.get(t),tfMap.get(t));
//			}
//		}
//	}
}