package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.Serialization2;

public class TestOutput {

	/**
	 * Provide Generic Map printing method
	 * @param mapp
	 * @param output
	 * @throws IOException
	 */
	public static <T, K> void printMap(HashMap<T,K> mapp, String output) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
		bw.flush();
		Iterator<Map.Entry<T,K>> mapIter =  mapp.entrySet().iterator();
		while(mapIter.hasNext()){
			Entry<T, K> entry = mapIter.next();
			bw.append(entry.getKey().toString());
			bw.append("\t");
			bw.append(entry.getValue().toString());
			bw.append("\n");
		}
		bw.close();
	}
	public static void printLinearTrend(String input, String output) throws IOException{
		String jaccardLinear = "/home/dock/Documents/IR/AmazonResults/mRange3/jaccardNon_Weighted.ser";
		String termBimap = "/home/dock/Documents/IR/AmazonResults/mRange3/ds/trimTermBimapm0.05.ser";
		
		HashSet<Integer> termSet =  (HashSet<Integer>) Serialization2.readFile(input);
		HashMap<Integer, HashMap<Integer, Double>> jlin = Serialization2.deserialize(jaccardLinear);
		HashBiMap<String, Integer> bimap = Serialization2.deserialize(termBimap);
		HashMap<Integer, Double> intervalCosetAvgTermSetSize = Serialization2.deserialize("/home/dock/Documents/IR/AmazonResults/mRange3/ds/IntervalCosetAvgSize.ser");
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
		
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