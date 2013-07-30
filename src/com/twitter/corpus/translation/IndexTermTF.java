package com.twitter.corpus.translation;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.twitter.corpus.types.SerializationHelper;

/**
 * 
 * Calculate term frequency values and serialise for Jaccard calculations
 *
 */
public class IndexTermTF {
	private static final Logger LOG = Logger.getLogger(IndexTermTF.class);
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException{
		
		String corpPath = "/trec/output/mRange3/trimGlobalIndex.ser";
		String tfMapPath = "/trec/output/mRange3/tfMap.ser";
				
		HashMap<Integer, HashSet<Long>> corpusIndex = SerializationHelper.deserialize(corpPath);
		
		HashMap<Integer, Integer> tfMap = new HashMap<Integer, Integer>(corpusIndex.size());
		
		Iterator<Entry<Integer, HashSet<Long>>> corpIter = corpusIndex.entrySet().iterator();
		while(corpIter.hasNext()){
			Entry<Integer, HashSet<Long>> entry = corpIter.next();
			tfMap.put(entry.getKey(), entry.getValue().size());
		}
		SerializationHelper.serialize(tfMap, tfMapPath);
	}
}