package com.twitter.corpus.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import com.twitter.corpus.types.CoWeight;

public class Jaccard {
	public static HashMap<Integer, Double> getJaccardSimilarity(ArrayList<HashMap<Integer, HashSet<CoWeight>>> cosetArray){
		double jaccard;
		// iterate one hashmap taking terms, get corresponding term set from other cosetarray and jaccard it
		// if the term doesnt exist in the otehr array skip but log error to console

		// return map, initialzed with size of 1st item in cosetArray
		HashMap<Integer, ArrayList<Double>> jaccardList = new HashMap<Integer, ArrayList<Double>>(cosetArray.get(0).size());

		// Get the union of the keys from both arrays
		ArrayList<HashMap<Integer, HashSet<CoWeight>>> copyCosetArray = new ArrayList<HashMap<Integer,HashSet<CoWeight>>>(cosetArray);
		copyCosetArray.get(0).keySet().retainAll(copyCosetArray.get(1).keySet());
		Iterator<Integer> termIterator = copyCosetArray.get(0).keySet().iterator();

		while(termIterator.hasNext()){
			// term under condsideration
			Integer term = termIterator.next();

			//cosets of term from different days
			HashSet<CoWeight> termCosetA = cosetArray.get(0).get(term);
			HashSet<CoWeight> termCosetB = cosetArray.get(1).get(term);

			if(termCosetB != null && termCosetA != null){
				int intersectionSize = termCosetA.size() > termCosetB.size() ? termCosetA.size():termCosetB.size();
				HashSet<CoWeight> intersection = new HashSet<CoWeight>(intersectionSize);
				HashSet<CoWeight> union = new HashSet<CoWeight>(intersectionSize);

				Iterator<CoWeight> aIter = termCosetA.iterator();
				while(aIter.hasNext()){
					CoWeight co = aIter.next();
					union.add(co);
					if(termCosetB.contains(co)){
						intersection.add(co);	
					}
				}
				termCosetB.removeAll(intersection);
				Iterator<CoWeight> iterB = termCosetB.iterator();
				while(iterB.hasNext()){
					CoWeight co = iterB.next();
					union.add(co);
					if(termCosetA.contains(co)){
						intersection.add(co);
					}
				}
				jaccard = (double)intersection.size() / (double) union.size();
			}
		}
		return null;		
	}
}