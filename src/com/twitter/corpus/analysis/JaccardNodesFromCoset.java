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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;

import com.twitter.corpus.types.CoWeight;

public class JaccardNodesFromCoset {
	private static final Logger LOG = Logger.getLogger(JaccardNodesFromCoset.class);
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

		// Jaccard all pairs
		int cutoff = 20;
		for(int i =0 ; i < corpusCoSetArray.size(); i++){
			for(int j =i ; j < corpusCoSetArray.size() - 1; j++){
				ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corp = new ArrayList<HashMap<Integer,ArrayList<CoWeight>>>(2);

				corp.add(corpusCoSetArray.get(j));
				corp.add(corpusCoSetArray.get(j+1));
				jaccardNodes.getJaccardSimilarityAllNodes(corpusCoSetArray, cutoff, i , j);
			}
		}
		
		
		Jaccard JaccardNodesTrimmed = new Jaccard(jaccardNodes.jaccardAllNodes.size());
		ArrayList<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> trimmedTerms = trimIZeros(jaccardNodes.jaccardAllNodes);
// rank the trimmed terms and then print a graph
		ArrayList<Map.Entry<Integer, Integer>> nonZeroMap = getNonZeros(trimmedTerms);
		
		// count non zeroz for each term
//		ArrayList<Map.Entry<Integer, Integer>> nonZeroNodeMap = getNonZeros(jaccardNodes.jaccardAllNodes);
		// print it out
		printNodeNonZeroRank(nonZeroMap, "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/nodeSizesNoIZero.csv");
		
		// get a set of terms to decode
		HashSet<Integer> termsToDecode = (HashSet<Integer>) TermBimapDeser.readFile("/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/20nonZeroNodes.csv");

		// print out jaccard all nodes for each term in set
		for(Integer term : termsToDecode){
			printForTerm(jaccardNodes.jaccardAllNodes, term,"/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/termNodeJ" );
		}
		

		Double threshold = 0.3;
		HashMap<Integer, Integer> keeptrack = new HashMap<Integer, Integer>();
		for(Integer term : termsToDecode){
			Integer count = thresholdCounter(jaccardNodes.jaccardAllNodes.get(term), threshold);
			keeptrack.put(term, count);
		}
		
		System.console();
		// iterate all nodes and rank by number that are above a certain threshold
		// threshold = 0.3

		// sort jaccard pairs by number of non-zero pairs
		//		ArrayList<Map.Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> sortedJNodes = sortNodesByZeroCount(jaccardNodes.jaccardAllNodes);
		// Print sorted jaccard nodes
		//		printNodeNonZeroCount(sortedJNodes,"/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/nodeSizes2.csv");

	}
	/**
	 * create a new jaccardAllNodes array by removing any term that is absent at any interval
	 * 
	 * @param jaccardAllNodes
	 */
	private static ArrayList<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> trimIZeros(HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> jaccardAllNodes){
		LOG.info("Old JaccardAllNodes size; " + jaccardAllNodes.size());
		ArrayList<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> retVal = new ArrayList<Map.Entry<Integer,HashMap<Integer,HashMap<Integer,Double>>>>(jaccardAllNodes.size());
		
		Iterator<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> jNodesIter = jaccardAllNodes.entrySet().iterator();
		while (jNodesIter.hasNext()) {
			Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>> entry = jNodesIter.next();
			Boolean isZero = false;		
			Iterator<Entry<Integer, HashMap<Integer, Double>>> iIter = entry.getValue().entrySet().iterator();
			while(iIter.hasNext()){
				Entry<Integer, HashMap<Integer, Double>> iEntry = iIter.next();
				Integer i = iEntry.getKey();
				HashMap<Integer, Double> value = iEntry.getValue();
				if(value.get(i) == 0.0){
					isZero = true;
					break;
				}
			}
			if(!isZero){
				retVal.add(entry);
			}
		}
		LOG.info("New jaccardAllNodes size: " + retVal.size());
		return retVal;
	}
	private static Integer thresholdCounter(HashMap<Integer, HashMap<Integer, Double>> termNode, Double threshold){
		Integer retVal = 0;
		Iterator<Map.Entry<Integer, HashMap<Integer, Double>>> termNodeIter = termNode.entrySet().iterator();
		while(termNodeIter.hasNext()){
			Entry<Integer, HashMap<Integer, Double>> entry = termNodeIter.next();
			HashMap<Integer, Double> asd = entry.getValue();
			
			Iterator<Map.Entry<Integer, Double>> asdIter = asd.entrySet().iterator();
			while(asdIter.hasNext()){
				Double jac = asdIter.next().getValue();
				if( jac < threshold && jac != 0.0){
					retVal++;
				}
			}
		}
		return retVal;
	}
	/**
	 * Print out 
	 * @param nonZeroNodeMap
	 * @param path
	 * @throws IOException
	 */
	private static void printForTerm(HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> nodes,Integer term, String path ) throws IOException{

		BufferedWriter bw = new BufferedWriter(new FileWriter(path + term + ".txt"));		

		HashMap<Integer, HashMap<Integer, Double>> TermSet = nodes.get(term);

		for(int i = 0; i < TermSet.size() ;i++){
			HashMap<Integer, Double> asd = TermSet.get(i);
			for(int j = i; j < asd.size()+i ;j++){
				if(asd.get(j) == 0.0){
					bw.append("-.-,");
				}
				else{
				bw.append(asd.get(j).toString() + ",");
				}
			}
			bw.newLine();
			bw.flush();
		}
		bw.close();
		
//		Iterator<Entry<Integer, HashMap<Integer, Double>>> termSetIter = TermSet.entrySet().iterator();
//		while(termSetIter.hasNext()){
//			Entry<Integer, HashMap<Integer, Double>> jEntry = termSetIter.next();
//			Iterator<Entry<Integer, Double>> jSet = jEntry.getValue().entrySet().iterator();
//			while(jSet.hasNext()){
//				Double j = jSet.next().getValue();
//				bw.append(j + ",");
//			}
//			bw.append("\n");
//		}
	}
	private static void printNodeNonZeroRank(ArrayList<Entry<Integer, Integer>> nonZeroNodeMap, String path) throws IOException{
		BufferedWriter bf =  new BufferedWriter(new FileWriter(path));
		Iterator<Entry<Integer, Integer>> sjIter = nonZeroNodeMap.iterator();
		while(sjIter.hasNext()){
			Entry<Integer, Integer> entry = sjIter.next();
			bf.append(entry.getKey() + "\t" + entry.getValue() + "\n");
		}
		bf.close();
	}
		
	private static ArrayList<Entry<Integer, Integer>> getNonZeros(ArrayList<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> nodes){
		HashMap<Integer, Integer> nodesMap = new HashMap<Integer, Integer>();
		
		Iterator<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> nodeIter = nodes.iterator();
		while(nodeIter.hasNext()){
			Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>> entry = nodeIter.next();
			Integer term = entry.getKey();
			Integer nonZeroSize = countNonZeros(entry.getValue());
			nodesMap.put(term, nonZeroSize);
		}

		ArrayList<Map.Entry<Integer, Integer>> sortedMap = new ArrayList<Map.Entry<Integer,Integer>>(nodesMap.size());
		Iterator<Map.Entry<Integer, Integer>> sizeMapIter = nodesMap.entrySet().iterator();
		while(sizeMapIter.hasNext()){
			sortedMap.add(sizeMapIter.next());
		}
		Collections.sort(sortedMap, new nonZeroComaparator());
		return sortedMap;
	}
	
	/**
	 * Get map of term to non-zero node count
	 * @param nodes
	 * @return
	 */
	private static ArrayList<Entry<Integer, Integer>> getNonZeros(HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> nodes){
		HashMap<Integer, Integer> nodesMap = new HashMap<Integer, Integer>();

		Iterator<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> nodeIter = nodes.entrySet().iterator();
		while(nodeIter.hasNext()){
			Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>> entry = nodeIter.next();
			Integer term = entry.getKey();
			Integer nonZeroSize = countNonZeros(entry.getValue());
			nodesMap.put(term, nonZeroSize);
		}

		ArrayList<Map.Entry<Integer, Integer>> sortedMap = new ArrayList<Map.Entry<Integer,Integer>>(nodesMap.size());
		Iterator<Map.Entry<Integer, Integer>> sizeMapIter = nodesMap.entrySet().iterator();
		while(sizeMapIter.hasNext()){
			sortedMap.add(sizeMapIter.next());
		}
		Collections.sort(sortedMap, new nonZeroComaparator());
		return sortedMap;
	}

	private static ArrayList<Map.Entry<Integer, ArrayList<Double>>> nonZeroPairs(ArrayList<Map.Entry<Integer, ArrayList<Double>>> sortedJNodes){
		ArrayList<Map.Entry<Integer, ArrayList<Double>>> nonZero = new ArrayList<Map.Entry<Integer,ArrayList<Double>>>(528);
		Iterator<Map.Entry<Integer, ArrayList<Double>>> sjIter2 = sortedJNodes.iterator();
		while(sjIter2.hasNext()){
			Entry<Integer, ArrayList<Double>> entry = sjIter2.next();
			if(entry.getValue().size() == 528){
				nonZero.add(entry);
			}
		}
		return nonZero;
	}
	private static void printTermRangeOFPairNonZeroes(ArrayList<Map.Entry<Integer, ArrayList<Double>>> nonZero, int lower, int upper, String path) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(path));
		Iterator<Map.Entry<Integer, ArrayList<Double>>> iterateForTerms = nonZero.iterator();
		while(iterateForTerms.hasNext()){
			Integer t = iterateForTerms.next().getKey();

			bw.append(t + "\n");			
		}
		bw.close();
	}
	private static void printNodeNonZeroCount(ArrayList<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> sortedJNodes, String path) throws IOException{
		BufferedWriter bf =  new BufferedWriter(new FileWriter(path));

		Iterator<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> sjIter = sortedJNodes.iterator();
		while(sjIter.hasNext()){
			Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>> entry = sjIter.next();

			bf.append(entry.getKey() + "\t" + entry.getValue().size() + "\n");
		}
		bf.close();
	}
	/**
	 * Sort jaccard pairs by number of non-zero pairs, theoretical max is achieved using the data
	 * @param nodes
	 * @return
	 */
	private static ArrayList<Map.Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> sortNodesByZeroCount(HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> nodes){
		ArrayList<Map.Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> sortedJNodes = new ArrayList<Map.Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>>(nodes.size());
		Iterator<Map.Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> jIter = nodes.entrySet().iterator();
		while(jIter.hasNext()){
			sortedJNodes.add(jIter.next());
		}
		Collections.sort(sortedJNodes, new JSizeComparator());
		return sortedJNodes;

	}
	private static int countNonZeros(HashMap<Integer, HashMap<Integer, Double>> nodes){
		Iterator<Entry<Integer, HashMap<Integer, Double>>> nodeIter = nodes.entrySet().iterator();
		Integer count= 0;
		while(nodeIter.hasNext()){
			Iterator<Entry<Integer, Double>> lastIter = nodeIter.next().getValue().entrySet().iterator();
			while(lastIter.hasNext()){
				if(lastIter.next().getValue() != 0.0){
					count++;
				}
			}
		}
		return count;
	}
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
	public static class JSizeComparator implements Comparator<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>>{
		@Override
		public int compare(Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>> o1, Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>> o2) {
			if(countNonZeros(o1.getValue())  > countNonZeros(o2.getValue()))
				return -1;
			else if(countNonZeros(o1.getValue()) < countNonZeros(o2.getValue()))
				return 1;
			else
				return 0;
		}
	}
	public static class nonZeroComaparator implements Comparator<Entry<Integer, Integer>>{
		@Override
		public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {

			if(o1.getValue() > o2.getValue())
				return -1;
			else if(o1.getValue() < o2.getValue())
				return 1;
			else
				return 0;
		}
	}
}