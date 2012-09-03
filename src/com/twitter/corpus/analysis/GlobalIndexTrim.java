package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;

import com.twitter.corpus.types.CoWeight;
import com.twitter.corpus.types.Serialization2;

public class GlobalIndexTrim {
	private static final Logger LOG = Logger.getLogger(GlobalIndexTrim.class);
	// read in from term cosets that have already been trimmed
	// do the deed on the global index
	public static void main(String[] args) throws IOException{
		String input = "/trec/output/mRange3/tc_0.05/";
		String root = "termCoset_";
		String base = ".ser";
		
		String corpPath = "/trec/output/mRange3/globalIndex.ser";
		String corpOut = "/trec/output/mRange3/trimGlobalIndex.ser";

		String[] filePaths = new String[33];

		for(int i = 0; i < 33 ; i++){
			filePaths[i] = input + root + (i+1) + base;
		}

		HashSet<Integer> terms = new HashSet<Integer>(1000);
		for(String path : filePaths){
			HashMap<Integer, ArrayList<CoWeight>> coset = Serialization2.deserialize(path);
			terms.addAll(coset.keySet());
		}
		
		HashMap<Integer, HashSet<Long>> gCorp = Serialization2.deserialize(corpPath);
		HashMap<Integer, HashSet<Long>> newGlobalCorp = new HashMap<Integer, HashSet<Long>>((int)gCorp.size()/2);
		for(Integer term : terms){
			if(gCorp.containsKey(term)){
				newGlobalCorp.put(term, gCorp.get(term));
			}
		}
		Serialization2.serialize(newGlobalCorp, corpOut);
	}
}
