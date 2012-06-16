package com.twitter.corpus.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.twitter.corpus.types.CoWeight;

public class Jaccard {
	public static HashMap<Integer, Double> getJaccardSimilarity(ArrayList<HashMap<Integer, ArrayList<CoWeight>>> cosetArray){
		
		// iterate one hashmap taking terms, get corresponding term set from other cosetarray and jaccard it
		// if the term doesnt exist in the otehr array skip but log error to console
		Double jaccard = 0;
		Iterator<Entry<Integer, ArrayList<CoWeight>>> iterA = cosetArray.get(0).entrySet().iterator();
		while(iterA.hasNext()){
			ArrayList<CoWeight> termCosetA = iterA.next().getValue();
			Integer termA = iterA.next().getKey();
			
			ArrayList<CoWeight> termCosetB = cosetArray.get(1).get(termA);
			if(termCosetB != null){
				// do the jaccarding
				jaccard = termCosetA.
				
			}
		}
		return jaccard;		
	}
}
