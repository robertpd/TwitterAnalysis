package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.CoWeight;
import com.twitter.corpus.types.SerializationHelper;

public class ShrinkTermBimap {

	public static void main(String[] args) throws IOException{
			
		String input = "/trec/output/mRange3/tc_0.05/";
		String root = "termCoset_";
		String base = ".ser";
				
		String newTermBimapPath = "/trec/output/mRange3/trimTermBimap.ser";
		String termBimapPath = "/trec/output/mRange3/termbimap.ser";
		String[] filePaths = new String[33];

		for(int i = 0; i < 33 ; i++){
			filePaths[i] = input + root + (i+1) + base;
		}

		HashSet<Integer> terms = new HashSet<Integer>(2000);
		for(String path : filePaths){
			HashMap<Integer, ArrayList<CoWeight>> coset = SerializationHelper.deserialize(path);
			terms.addAll(coset.keySet());
		}
		
		HashBiMap<String, Integer> termBimap = SerializationHelper.deserialize(termBimapPath);
		HashBiMap<String, Integer> newTermBimap = HashBiMap.create(termBimap.size());
		for(Integer term : terms){
			newTermBimap.put(termBimap.inverse().get(term), term);
		}
		SerializationHelper.serialize(newTermBimap, newTermBimapPath);		
	}
}