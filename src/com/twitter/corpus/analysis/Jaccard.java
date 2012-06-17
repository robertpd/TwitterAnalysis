package com.twitter.corpus.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.twitter.corpus.types.CoWeight;

public class Jaccard {
	public Jaccard(int size){
		jaccardList = new HashMap<Integer, ArrayList<Double>>(size);
	}
	private static int dayCounter = 0;
	public static HashMap<Integer, ArrayList<Double>> jaccardList;
	
	/*** 
	 * 
	 * @param feed array of cosets for "today" and "yesterday"
	 * @return maintains a static hashMap of jaccards for each term over duration of corpus, access when finished traversing the corpus
	 */
	public static void getJaccardSimilarity(ArrayList<HashMap<Integer, HashMap<Integer, Double>>> cosetArray){
		double jaccard;
		// return map, initialzed with size of 1st item in cosetArray
//		HashMap<Integer, ArrayList<Double>> jaccardList = new HashMap<Integer, ArrayList<Double>>(cosetArray.get(0).size());

		// Get the union of the keys from both arrays
		ArrayList<HashMap<Integer, HashMap<Integer, Double>>> copyMap = new ArrayList<HashMap<Integer,HashMap<Integer, Double>>>(cosetArray);
		// this should create a new hashset and copy over the terms from the cosetArray...
		Set<Integer> termSet = new HashSet<Integer>(copyMap.get(0).keySet());
		termSet.addAll(copyMap.get(1).keySet());
		copyMap = null;				

		Iterator<Integer> termIterator = termSet.iterator();
		// iterate terms from both maps.. 
		while(termIterator.hasNext()){
			// term under condsideration
			Integer term = termIterator.next();

			// TODO sort out programming style
			
			//cosets of term from different days
			// TODO need to remove elements as they are processed to free up memory
			HashMap<Integer, Double> termCosetA = cosetArray.get(0).get(term);
			HashMap<Integer, Double> termCosetB = cosetArray.get(1).get(term);

			if(termCosetB != null && termCosetA != null){
				int intersectionSize = termCosetA.size() > termCosetB.size() ? termCosetA.size():termCosetB.size();
				HashSet<Integer> intersection = new HashSet<Integer>(intersectionSize);
				HashSet<Integer> union = new HashSet<Integer>(2*intersectionSize);

				// iterate A, get coweight and check i
				Iterator<Entry<Integer, Double>> aIter = termCosetA.entrySet().iterator();
				while(aIter.hasNext()){
					Map.Entry<Integer, Double> co = aIter.next();
					union.add(co.getKey());
					if(termCosetB.containsKey(co.getKey())){
						intersection.add(co.getKey());
					}
				}
				termCosetB.keySet().removeAll(intersection); // maybe ignore this step and just nullify the entire element at the end??
				Iterator<Entry<Integer, Double>> iterB = termCosetB.entrySet().iterator();
				while(iterB.hasNext()){
					Map.Entry<Integer, Double> co = iterB.next();
					union.add(co.getKey());
					if(termCosetA.containsKey(co.getKey())){
						intersection.add(co.getKey());
					}
				}
				jaccard = (double)intersection.size() / (double) union.size();
				if(!jaccardList.containsKey(term)){
					ArrayList<Double> jEachDay  =new ArrayList<Double>(33);
					jEachDay.add(dayCounter, jaccard);
					jaccardList.put(term, jEachDay);
				}
				else{
					jaccardList.get(term).add(dayCounter, jaccard);
				}
			}
		}
		dayCounter++; // must be incremented here to ensure continuity
	}
}