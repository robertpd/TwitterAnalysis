package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class VolatilityAnalysis {
	
	public static void avgVolatility(HashMap<Integer, HashMap<Integer,Double>> jaccard, String path) throws IOException{
	
		HashMap<Integer, Double> avgVolMap = new HashMap<Integer, Double>(jaccard.size());
		HashMap<Integer, Double> stdDevMap = new HashMap<Integer, Double>(jaccard.size());
		// iterate terms
		Iterator<Entry<Integer, HashMap<Integer, Double>>> iter = jaccard.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, HashMap<Integer, Double>> entry = iter.next();
			Integer term = entry.getKey();
			
			// iterate termsets
			Iterator<Map.Entry<Integer, Double>> jaccEntry = entry.getValue().entrySet().iterator();
			Double avgVol = 0.0;
			
			// calc mean
			
			while (jaccEntry.hasNext()) {
				avgVol += jaccEntry.next().getValue();
			}
			avgVol /= 32;
			avgVol = (double)Math.round(avgVol * 1000) / 1000;
			avgVolMap.put(term, avgVol);
			
			// using mean calculate standard deviation			
			
			Iterator<Map.Entry<Integer, Double>> jaccEntry2 = entry.getValue().entrySet().iterator();

			Double num = 0.0;
			while (jaccEntry2.hasNext()) {
				// sqrt of the absolute value
				num += Math.pow((Math.abs(jaccEntry2.next().getValue() - avgVol)), 2);
			}
			
			Double stdDev = Math.sqrt(num / 32);
			stdDev = (double)Math.round(stdDev * 1000) / 1000;
			
			stdDevMap.put(term, stdDev);
		}
		String avgVol = "_avgVol.csv";
		String stdDev = "_stdDev.csv";
		printAnalysisResults(avgVolMap, (path + avgVol));
		printAnalysisResults(stdDevMap, (path + stdDev));
	}
	public static void printAnalysisResults(HashMap<Integer, Double> avgVol, String pathname) throws IOException{
		
		BufferedWriter bf =  new BufferedWriter(new FileWriter(pathname));
			
		Iterator<Map.Entry<Integer, Double>> volIter = avgVol.entrySet().iterator();
		while(volIter.hasNext()){
			Entry<Integer, Double> entry = volIter.next();
			bf.append(entry.getKey() + "\t" + entry.getValue() + "\n");
		}
	}
}
