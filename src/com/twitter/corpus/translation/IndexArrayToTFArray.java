package com.twitter.corpus.translation;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.twitter.corpus.types.SerializationHelper;

/** 
 * Calculate size of term index entries and output size for each term
 * @author dock
 *
 */
public class IndexArrayToTFArray {
	private static final Logger LOG = Logger.getLogger(IndexArrayToTFArray.class);
	public static void main(String[] args) throws IOException{
		String corpPath = "/trec/output/mRange3/LocalIndexArray.ser";
		String tfMapPath = "/trec/output/mRange3/tfMap.ser";
		String tfMapArrayPath = "/trec/output/mRange3/tfMapArray.ser";
				
		LOG.info("Deserializing index array");
		ArrayList<HashMap<Integer, HashSet<Long>>> corpusIndex = SerializationHelper.deserialize(corpPath);
		LOG.info("Fin");
		LOG.info("Deserializing tfMap");
		HashMap<Integer, Integer> tfMap = SerializationHelper.deserialize(tfMapPath);
		LOG.info("Fin");

		ArrayList<HashMap<Integer, Integer>> retVal = new ArrayList<HashMap<Integer,Integer>>();
		for(int i =0 ; i < 33; i++){
			HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
			retVal.add(map);
		}

		LOG.info("Filtering terms for tfMap");
		Iterator<Entry<Integer, Integer>> tfMapIter = tfMap.entrySet().iterator();
		while(tfMapIter.hasNext()){
			Entry<Integer, Integer> entry = tfMapIter.next();
			Integer term = entry.getKey();

			for(int i =0; i < corpusIndex.size(); i++){
				HashMap<Integer, HashSet<Long>> intervalIndex = corpusIndex.get(i);
				if(intervalIndex.containsKey(term)){
					HashSet<Long> hashSetEntry = intervalIndex.get(term);
					Integer size = hashSetEntry.size();
					retVal.get(i).put(term, size);
				}
			}
		}
		LOG.info("Fin");
		LOG.info("Serializing tfMapArray");
		
		FileOutputStream fileOut = new FileOutputStream(tfMapArrayPath);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(retVal);
		objectOut.close();
		
//		Serialization2.serialize(retVal, tfMapArrayPath);
		LOG.info("Fin all");
	}
}