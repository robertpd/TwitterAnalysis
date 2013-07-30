package com.twitter.corpus.experiments;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.mockito.internal.stubbing.answers.Returns;

import com.twitter.corpus.types.SerializationHelper;

/**
 * Calculate the average linear stability
 * 
 *
 */
public class AvgStabilityLinear {

	public static HashMap<Integer, Double> stdDevMap;
	public static void main (String args[]) throws IOException{
		String outputAvg = "/home/dock/Documents/IR/AmazonResults/mRange3/avgStab.ser";
		String outputStdDev = "/home/dock/Documents/IR/AmazonResults/mRange3/stdDev.ser";
		double jacc = 0.0;
		
		String jaccardLinear = "/home/dock/Documents/IR/AmazonResults/mRange3/jaccardNon_Weighted.ser";
		HashMap<Integer, HashMap<Integer, Double>> jlin = SerializationHelper.deserialize(jaccardLinear);

		HashMap<Integer, Double> avgStabMap = new HashMap<Integer, Double>(jlin.size());
		
		Iterator<Entry<Integer, HashMap<Integer, Double>>> jaccIter = jlin.entrySet().iterator();
		while(jaccIter.hasNext()){
			Entry<Integer, HashMap<Integer, Double>> termJacc = jaccIter.next();

			Iterator<Entry<Integer, Double>> termJaccIter = termJacc.getValue().entrySet().iterator();

			while(termJaccIter.hasNext()){
				jacc += termJaccIter.next().getValue();
			}
			jacc /= 32;
			jacc = (double)Math.round(jacc * 1000) / 1000;
			avgStabMap.put(termJacc.getKey(), jacc);
			jacc=0.0;
		}
		SerializationHelper.serialize(avgStabMap, outputAvg);		
		
		stdDevMap = new HashMap<Integer, Double>(avgStabMap.size());
		
		Iterator<Entry<Integer, HashMap<Integer, Double>>> jaccIterStdDev = jlin.entrySet().iterator();
		while(jaccIterStdDev.hasNext()){
			Entry<Integer, HashMap<Integer, Double>> termJacc2 = jaccIterStdDev.next();
			int term = termJacc2.getKey();

			Iterator<Entry<Integer, Double>> termJaccIter = termJacc2.getValue().entrySet().iterator();
			double num = 0.0;
			while(termJaccIter.hasNext()){
				num += Math.pow((Math.abs(termJaccIter.next().getValue() - avgStabMap.get(term))), 2);
			}
			Double stdDev = Math.sqrt(num / 32);
			stdDev = (double)Math.round(stdDev * 1000) / 1000;
			
			stdDevMap.put(termJacc2.getKey(), stdDev);
			
		}
		SerializationHelper.serialize(stdDevMap, outputStdDev);
		
		HashMap<Integer, Double> trend = new HashMap<Integer, Double>();
		
		BufferedWriter bwa = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/avgStab.csv"));
		Iterator<Entry<Integer, Double>> avgIter = avgStabMap.entrySet().iterator();
		while(avgIter.hasNext()){
			Entry<Integer, Double> as = avgIter.next();
			bwa.append(as.getKey() + "," + as.getValue() + "\n");
		}
		bwa.close();
		
		BufferedWriter bws = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/stdStab.csv"));
		Iterator<Entry<Integer, Double>> stdIter = stdDevMap.entrySet().iterator();
		while(stdIter.hasNext()){
			Entry<Integer, Double> as = stdIter.next();
			bws.append(as.getKey() + "," + as.getValue() + "\n");
		}
		bws.close();
	}
	public static class rankedComparator implements Comparator<Map.Entry<Integer, Double>> {
		@Override
		public int compare(Entry<Integer, Double> arg0,Entry<Integer, Double> arg1) {
			if(stdDevMap.get(arg0.getKey())  < stdDevMap.get(arg1.getKey())){
				return -1;
			}
			if(stdDevMap.get(arg0.getKey())  == stdDevMap.get(arg1.getKey()))
					return 0;
			else
			return 1;
		}	
	}
}