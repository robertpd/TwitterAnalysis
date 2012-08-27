package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.tools.GetVersion;

import com.twitter.corpus.types.CoWeight;

public class JaccardNodesFromCoset {
	public static void main(String[] args) throws IOException{

		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/";
		String root = "termCoset_";
		String base = ".ser";

		String[] filePaths = new String[33];

		for(int i = 0; i < 33 ; i++){
			filePaths[i] = input + root + (i+1) + base;
		}

		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corpusCoSetArray = new ArrayList<HashMap<Integer, ArrayList<CoWeight>>>(33);

		for(String path : filePaths){
			String cosetPath = path;
			HashMap<Integer, ArrayList<CoWeight>> coset = deserialize(cosetPath);
			corpusCoSetArray.add(coset);
		}

		Jaccard jaccardNodes = new Jaccard(corpusCoSetArray.get(1).size(), "Nodes");

		int cutoff = 20;
		for(int i =0 ; i < corpusCoSetArray.size(); i++){
			for(int j =i ; j < corpusCoSetArray.size() - 1; j++){
				ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corp = new ArrayList<HashMap<Integer,ArrayList<CoWeight>>>(2);

				corp.add(corpusCoSetArray.get(j));
				corp.add(corpusCoSetArray.get(j+1));
				jaccardNodes.getJaccardSimilarityAllNodes(corp, cutoff);
			}
		}

		ArrayList<Map.Entry<Integer, ArrayList<Double>>> sortedJNodes = new ArrayList<Map.Entry<Integer,ArrayList<Double>>>(jaccardNodes.jaccardNodes.size());
		Iterator<Map.Entry<Integer, ArrayList<Double>>> jIter = jaccardNodes.jaccardNodes.entrySet().iterator();
		while(jIter.hasNext()){
			sortedJNodes.add(jIter.next());
		}
		Collections.sort(sortedJNodes, new JSizeComparator());
		
		// Print sorted jaccard nodes
		BufferedWriter bf =  new BufferedWriter(new FileWriter("/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/nodeSizes.csv"));
		Iterator<Map.Entry<Integer, ArrayList<Double>>> sjIter = sortedJNodes.iterator();
		while(sjIter.hasNext()){
			Entry<Integer, ArrayList<Double>> entry = sjIter.next();
			bf.append(entry.getKey() + "\t" + entry.getValue().size() + "\n");
		}
		bf.close();
		
		ArrayList<Map.Entry<Integer, ArrayList<Double>>> nonZero = new ArrayList<Map.Entry<Integer,ArrayList<Double>>>(528);
		Iterator<Map.Entry<Integer, ArrayList<Double>>> sjIter2 = sortedJNodes.iterator();
		while(sjIter2.hasNext()){
			Entry<Integer, ArrayList<Double>> entry = sjIter2.next();
			if(entry.getValue().size() == 528){
				nonZero.add(entry);
			}
		}
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/node568TermInts.csv"));
		Iterator<Map.Entry<Integer, ArrayList<Double>>> iterateForTerms = nonZero.iterator();
		while(iterateForTerms.hasNext()){
			Integer t = iterateForTerms.next().getKey();
			bw.append(t + "\n");			
		}
		bw.close();
		
		ArrayList<Double> avgArray = new ArrayList<Double>();
		HashMap<Integer, Integer> volMap = new HashMap<Integer, Integer>(sortedJNodes.size());
		Iterator<Map.Entry<Integer, ArrayList<Double>>> nonZeroIter = nonZero.iterator();
		while(nonZeroIter.hasNext()){
			Map.Entry<Integer, ArrayList<Double>> entry = nonZeroIter.next();
			Integer term = entry.getKey();
			ArrayList<Double> jNodes = entry.getValue();
			
			Double min = 1.0;
			Double max = 0.0;
			Double subMax = 0.0;
			Double subMin = 0.0;
			Double range = 0.0;
			
			int count = 0;
			Double average = 0.0;
			
			Iterator<Double> jNIter = jNodes.iterator();
			while(jNIter.hasNext()){
				Double j = jNIter.next();
				average += j;
//				if(j < min){
//					min = j;
//				}
//				if(j > max){
//					max = j;
//				}
				count++;
			}
			average = average / (double)count;
			avgArray.add(average);			
//			range = max - min;
//			subMin = min * 1.1;
//			subMax = max * 0.9;
//			int vCount = 0;
//			Iterator<Double> secondIter = jNodes.iterator();
//			while(secondIter.hasNext()){
//				Double j = secondIter.next();
//				if(j > subMax || j< subMin){
//					vCount++;
//				}
//			}
//			volMap.put(term, vCount);
		}
		BufferedWriter bf2 =  new BufferedWriter(new FileWriter("/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/nodeAvg.csv"));
		Iterator<Double> avgArrayIter = avgArray.iterator();
		while(avgArrayIter.hasNext()){
			Double entry = avgArrayIter.next();
			bf2.append(entry + "\n");
		}
		bf2.close();
	}
	//	private HashMap<>
	@SuppressWarnings("unchecked")
	public static HashMap<Integer, ArrayList<CoWeight>> deserialize(String path){
		File cosetFile = new File(path);

		HashMap<Integer, ArrayList<CoWeight>> retVal = null;
		try{			
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(cosetFile));
			retVal = (HashMap<Integer, ArrayList<CoWeight>>) in.readObject();
			in.close();
		}
		catch(Exception ex){
			// gulp
		}
		return retVal;
	}
	public static class JSizeComparator implements Comparator<Entry<Integer, ArrayList<Double>>>{
		@Override
		public int compare(Entry<Integer, ArrayList<Double>> o1, Entry<Integer, ArrayList<Double>> o2) {
			if(o1.getValue().size() > o2.getValue().size())
				return -1;
			else if(o1.getValue().size() < o2.getValue().size())
				return 1;
			else
				return 0;
		}
	}
}