package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.twitter.corpus.types.Serialization2;

public class TestLocalIndex {
	private static final Logger LOG = Logger.getLogger(TestLocalIndex.class);
	public static void main(String[] args) throws IOException{
		String corpPath = "/trec/output/mRange3/LocalIndexArray.ser";
		String outPath = "/trec/output/mRange3/li/";
		
		int counter = 0;
		LOG.info("deser local index");
		ArrayList<HashMap<Integer, HashSet<Long>>> corpusIndex = Serialization2.deserialize(corpPath);
		LOG.info("Fin");
		Iterator<HashMap<Integer, HashSet<Long>>> coIt = corpusIndex.iterator();
		while(coIt.hasNext()){
			HashMap<Integer, HashSet<Long>> entry = coIt.next();
			Serialization2.serialize(entry, outPath + counter + ".ser");
			LOG.info("Local Index no. " + counter + " size: " + entry.size());
			counter++;
		}
		LOG.info("Fin all");
	}
}