package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import com.twitter.corpus.types.Serialization2;

public class IndexArrayToTFArray {
	public static void main(String[] args) throws IOException{
		String corpPath = "/trec/output/mRange3/LocalIndexArray.ser";
		String tfMapPath = "/trec/output/mRange3/tfMap.ser";
		String tfMapArrayPath = "/trec/output/mRange3/tfMapArray.ser";

		ArrayList<HashMap<Integer, HashSet<Long>>> corpusIndex = Serialization2.deserialize(corpPath);
		HashMap<Integer, Integer> tfMap = Serialization2.deserialize(tfMapPath);

		ArrayList<HashMap<Integer, Integer>> retVal = new ArrayList<HashMap<Integer,Integer>>();
		for(int i =0 ; i < 34; i++){
			HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
			retVal.add(map);
		}

		Iterator<Entry<Integer, Integer>> tfMapIter = tfMap.entrySet().iterator();
		while(tfMapIter.hasNext()){
			Entry<Integer, Integer> entry = tfMapIter.next();
			Integer term = entry.getKey();

			for(int i =0; i < corpusIndex.size(); i++){
				HashMap<Integer, HashSet<Long>> indexEntry = corpusIndex.get(i);
				if(indexEntry.containsKey(term)){
					HashSet<Long> hashMapEntry = indexEntry.get(term);
					Integer size = hashMapEntry.size();
					retVal.get(i).put(term, size);
				}
			}
		}
		Serialization2.serialize(retVal, tfMapArrayPath);
	}
}