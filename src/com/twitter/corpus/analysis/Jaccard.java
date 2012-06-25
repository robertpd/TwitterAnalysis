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
	public static void getJaccardSimilarity(ArrayList<HashMap<Integer, HashMap<Integer, Double>>> cosetArraya){
		double jaccard;
		ArrayList<HashMap<Integer, HashMap<Integer, Double>>> cosetArray = new ArrayList<HashMap<Integer,HashMap<Integer,Double>>>(2);
		HashMap<Integer, HashMap<Integer, Double>> one = new HashMap<Integer, HashMap<Integer,Double>>(5);
		HashMap<Integer, HashMap<Integer, Double>> two = new HashMap<Integer, HashMap<Integer,Double>>(5);
		
		// map of coweights
		HashMap<Integer, Double> subOne = new HashMap<Integer, Double>(7);
		// coweights
		subOne.put(0, 1.1);
		subOne.put(1, 1.1);
		subOne.put(2, 1.1);
		subOne.put(3, 1.1);
		subOne.put(4, 1.1);
		subOne.put(5, 1.1);
		subOne.put(6, 1.1);
		HashMap<Integer, Double> subTwo = new HashMap<Integer, Double>(7);
		subTwo.put(0, 1.1);
		subTwo.put(1, 1.1);
		subTwo.put(2, 1.1);
		subTwo.put(3, 1.1);
		subTwo.put(10, 1.1);
		subTwo.put(20, 1.1);
		subTwo.put(30, 1.1);
		one.put(0, subOne);
		two.put(2, subTwo);
		
		cosetArray.add(one);
		cosetArray.add(two);
		
		// Get the union of the keys from both arrays
		ArrayList<HashMap<Integer, HashMap<Integer, Double>>> copyMap = new ArrayList<HashMap<Integer,HashMap<Integer, Double>>>(cosetArray);
		
		// this should create a new hashset and copy over the terms from the cosetArray...
		
		// union set is set of terms(keys) that occur in both hashsets
		// incorrect: union set is set of outer keys, not correlates
		// need to get innet set of correlates to get the union
		Set<Integer> allTerms = new HashSet<Integer>(copyMap.get(0).keySet());
		// need intersection set to keep track of what terms have been checked
		// Set is good as it wont add a key already present
		// cant use simple int incremenet for intersection
		Set<Integer> interSet = null;//new HashSet<Integer>();
		allTerms.addAll(copyMap.get(1).keySet());
		copyMap = null;

		// now have all unique keys and can iterate
		Iterator<Integer> termIterator = allTerms.iterator();
		
		while(termIterator.hasNext()){
			Integer term = termIterator.next();
			interSet = new HashSet<Integer>(cosetArray.get(0).get(term).size());

			HashMap<Integer, Double> termCosetA = cosetArray.get(0).get(term);
			HashMap<Integer, Double> termCosetB = cosetArray.get(1).get(term);
			
			// chk != null
			// if term doesn't occur on particular day (unlightly, given thresholds performed already), termCoset will be null, this would give a 0 jaccard
			if(termCosetB != null && termCosetA != null){
				// iterate A, get coweight and check i
				Iterator<Entry<Integer, Double>> aIter = termCosetA.entrySet().iterator();
				while(aIter.hasNext()){
					Map.Entry<Integer, Double> co = aIter.next();
					if(termCosetB.containsKey(co.getKey())){
						interSet.add(co.getKey());
					}
				}
				Iterator<Entry<Integer, Double>> iterB = termCosetB.entrySet().iterator();
				while(iterB.hasNext()){
					Map.Entry<Integer, Double> co = iterB.next();
					if(termCosetA.containsKey(co.getKey())){
						interSet.add(co.getKey());
					}
				}
				jaccard = (double)interSet.size() / (double) allTerms.size();
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