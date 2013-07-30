package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.twitter.corpus.types.CoWeight;
import com.twitter.corpus.types.SerializationHelper;

public class JaccardNodesFromCoset {
	private static final Logger LOG = Logger.getLogger(JaccardNodesFromCoset.class);
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException{

		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/tc/";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/jnodes/";
		String[] filePaths = new String[33];
		
		for(int i = 0; i < 33 ; i++){
			filePaths[i] = input + "termCoset_" + (i+1) + ".ser";	}

		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corpusCoSetArray = new ArrayList<HashMap<Integer, ArrayList<CoWeight>>>(33);
		for(String path : filePaths){	
			corpusCoSetArray.add((HashMap<Integer, ArrayList<CoWeight>>) SerializationHelper.deserialize(path));
		}

		Jaccard jaccardNodes = new Jaccard(corpusCoSetArray.get(1).size(), "Nodes");
		// Jaccard all pairs
		int cutoff = 20;
		for(int i =0 ; i < corpusCoSetArray.size(); i++){
			for(int j =i ; j < corpusCoSetArray.size() - 1; j++){
				jaccardNodes.getJaccardSimilarityAllNodes(corpusCoSetArray, cutoff, i , j);
			}
		}
//		Serialization2.serialize(jaccardNodes.jaccardAllNodes, output+"jaccardAllNodes.ser");
		
		Jaccard jaccardIntervals = new Jaccard(corpusCoSetArray.get(1).size());
		jaccardIntervals.getJaccardSimilarity(corpusCoSetArray, cutoff);
//		Serialization2.serialize(jaccardIntervals.jaccardListNonWeighted, output+"jaccardIntervals.ser");
		
		String p = output+"jaccardIntervals.ser";
		
		LOG.info("Serializing...");
		FileOutputStream fileOut = new FileOutputStream(p);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(jaccardIntervals.jaccardListNonWeighted);
		objectOut.close();
		LOG.info("Finished serializing");
		

		String ppath = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/all/threshMap";
		
		for(Double i = 0.3; i <= 0.3 ; i+=0.1){
			HashMap<Integer, Integer> thresh = new HashMap<Integer, Integer>();
			Iterator<Map.Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> threshTermsIter = jaccardNodes.jaccardAllNodes.entrySet().iterator();//jaccardNodeThresholdTerms.entrySet().iterator();
			while(threshTermsIter.hasNext()){
				Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>> entry = threshTermsIter.next();
				Integer t = entry.getKey();
				HashMap<Integer, HashMap<Integer, Double>> val = entry.getValue();
				thresh.put(t, thresholdCounterDiscountIZero(val, i));
			}
			SerializationHelper.serialize(thresh, ppath + i + ".ser");
			BufferedWriter bw = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/all/" + i + "thresh.csv"));
			Iterator<Entry<Integer, Integer>> threshI = thresh.entrySet().iterator();
			while(threshI.hasNext()){
				Entry<Integer, Integer> entry = threshI.next();
				bw.append(entry.getKey() + "," + entry.getValue() + "\n");
			}
			bw.close();
		}

		// remove terms with a term absence at any interval
		ArrayList<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> trimmedTerms = trimIZeros(jaccardNodes.jaccardAllNodes);
		// rank the trimmed terms and then print a graph
		ArrayList<Map.Entry<Integer, Integer>> nonZeroMap = getNonZeros(trimmedTerms);
		printNodeNonZeroRank(nonZeroMap, "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/nodeSizesNoIZero.csv");

		// get a set of terms to decode
		HashSet<Integer> termsToDecode = (HashSet<Integer>) SerializationHelper.readFile("/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/20nonZeroNodes.csv");

		Double threshold2 = 0.3;
		HashMap<Integer, Integer> keeptrack = new HashMap<Integer, Integer>();
		for(Integer term : termsToDecode){
			Integer count = thresholdCounter(jaccardNodes.jaccardAllNodes.get(term), threshold2);
			keeptrack.put(term, count);
		}

		System.console();
	}

	/**
	 * create a new jaccardAllNodes array by removing any term that is absent at any interval
	 * 
	 * @param jaccardAllNodes
	 */
	private static ArrayList<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> trimIZeros(HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> jaccardAllNodes){
		LOG.info("Old JaccardAllNodes size; " + jaccardAllNodes.size());
		ArrayList<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> retVal = new ArrayList<Map.Entry<Integer,HashMap<Integer,HashMap<Integer,Double>>>>(jaccardAllNodes.size());

		Iterator<Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>>> termLevelIter = jaccardAllNodes.entrySet().iterator();
		while (termLevelIter.hasNext()) {
			Entry<Integer, HashMap<Integer, HashMap<Integer, Double>>> termEntry = termLevelIter.next();
			Boolean isZero = false;		
			Iterator<Entry<Integer, HashMap<Integer, Double>>> iLevelIter = termEntry.getValue().entrySet().iterator();
			// iterate at the i level, no further!
			while(iLevelIter.hasNext()){
				Entry<Integer, HashMap<Integer, Double>> iEntry = iLevelIter.next();
				Integer i = iEntry.getKey();
				HashMap<Integer, Double> value = iEntry.getValue();
				if(value.get(i) == 0.0){
					isZero = true;
					break;
				}
			}
			if(!isZero){
				retVal.add(termEntry);
			}
		}
		LOG.info("New jaccardAllNodes size: " + retVal.size());
		return retVal;
	}

	/**
	 * Count number of jaccard values passing a threshold. Questionable: Why discount zero j's? they are way passed the threshold and should be included?
	 * @param termNode
	 * @param threshold
	 * @return
	 */
	private static Integer thresholdCounterDiscountIZero(HashMap<Integer, HashMap<Integer, Double>> termNode, Double threshold){
		Integer retVal = 0;
		Iterator<Map.Entry<Integer, HashMap<Integer, Double>>> termNodeIter = termNode.entrySet().iterator();
		while(termNodeIter.hasNext()){
			Entry<Integer, HashMap<Integer, Double>> entry = termNodeIter.next();
			Integer i = entry.getKey();
			HashMap<Integer, Double> asd = entry.getValue();
			Iterator<Map.Entry<Integer, Double>> asdIter = asd.entrySet().iterator();
			while(asdIter.hasNext()){
				Entry<Integer, Double> entryJ = asdIter.next();
				Integer j = entryJ.getKey(); 
				Double jac = entryJ.getValue();
				if( j == i && jac == 0.0){	// catch case where i jac is zero
//					retVal -= (i-1);	//reverse the effect
					retVal -= (32-j);
//					retVal -= 32;
					break;
				}
				else{
					if( jac < threshold ){
						retVal++;
					}
				}
			}
		}
		return retVal;
	}

	/**
	 * Count the number of jaccard values that pass a threshold
	 * @param termNode
	 * @param threshold
	 * @return
	 */
	private static Integer thresholdCounter(HashMap<Integer, HashMap<Integer, Double>> termNode, Double threshold){
		Integer retVal = 0;
		Iterator<Map.Entry<Integer, HashMap<Integer, Double>>> termNodeIter = termNode.entrySet().iterator();
		while(termNodeIter.hasNext()){
			Entry<Integer, HashMap<Integer, Double>> entry = termNodeIter.next();
			HashMap<Integer, Double> asd = entry.getValue();

			Iterator<Map.Entry<Integer, Double>> asdIter = asd.entrySet().iterator();
			while(asdIter.hasNext()){
				Double jac = asdIter.next().getValue();
				if( jac < threshold /*&& jac != 0.0*/){
					retVal++;
				}
			}
		}
		return retVal;
	}
	/**
	 * Print out Term Jaccard Triangle
	 * @param nonZeroNodeMap
	 * @param path
	 * @throws IOException
	 */
	private static void printTermTriangle(HashMap<Integer, HashMap<Integer, HashMap<Integer, Double>>> nodes,Integer term, String path ) throws IOException{

		BufferedWriter bw = new BufferedWriter(new FileWriter(path + term + ".txt"));		
		HashMap<Integer, HashMap<Integer, Double>> TermSet = nodes.get(term);
		for(int i = 0; i < TermSet.size() ;i++){
			HashMap<Integer, Double> asd = TermSet.get(i);
			for(int j = i; j < asd.size()+i ;j++){
				if(asd.get(j) == 0.0){	bw.append("-.-,");	}
				else{	bw.append(asd.get(j).toString() + ",");	}
			}
			bw.newLine();bw.flush();
		}
		bw.close();
	}
	
	/**
	 * print out hashmap of terms ranked by the number of non-zero jaccards calculated by jaccardallnodes
	 * @param nonZeroNodeMap
	 * @param path
	 * @throws IOException
	 */
	private static void printNodeNonZeroRank(ArrayList<Entry<Integer, Integer>> nonZeroNodeMap, String path) throws IOException{
		BufferedWriter bf =  new BufferedWriter(new FileWriter(path));
		Iterator<Entry<Integer, Integer>> sjIter = nonZeroNodeMap.iterator();
		while(sjIter.hasNext()){
			Entry<Integer, Integer> entry = sjIter.next();
			bf.append(entry.getKey() + "\t" + entry.getValue() + "\n");
		}
		bf.close();
	}

	/**
	 * return map of terms and count of non-zero jac pairs
	 * @param nodes
	 * @return
	 */
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

	/**
	 * Get just the 528 non-zero terms
	 * @param sortedJNodes
	 * @return
	 */
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
	/**
	 * Print out terms and their non-zero jaccard count. terms are ranked from high to low
	 * @param sortedJNodes
	 * @param path
	 * @throws IOException
	 */
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
	/**
	 * Count number of non-zero jaccard values for a term
	 * @param nodes
	 * @return
	 */
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