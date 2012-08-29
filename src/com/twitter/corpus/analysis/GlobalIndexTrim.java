package com.twitter.corpus.analysis;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;

import com.twitter.corpus.types.CoWeight;

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
			HashMap<Integer, ArrayList<CoWeight>> coset = deserialize(path);
			terms.addAll(coset.keySet());
		}
		
		HashMap<Integer, HashSet<Long>> gCorp = gCorpDeser(corpPath);
		HashMap<Integer, HashSet<Long>> newGlobalCorp = new HashMap<Integer, HashSet<Long>>((int)gCorp.size()/2);
		for(Integer term : terms){
			if(gCorp.containsKey(term)){
				newGlobalCorp.put(term, gCorp.get(term));
			}
		}
		
		serializeNewCorpus(newGlobalCorp, corpOut);
	}
	private static void serializeNewCorpus(HashMap<Integer, HashSet<Long>> newGlobalCorp, String gpath) throws IOException{
		LOG.info("serializing new Global Corpus Index.");
		FileOutputStream fileOut = new FileOutputStream(gpath);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(newGlobalCorp);
		objectOut.close();
	}
	@SuppressWarnings("unchecked")
	public static HashMap<Integer, ArrayList<CoWeight>> deserialize(String path){
		LOG.info("deser cosets");
		File cosetFile = new File(path);

		HashMap<Integer, ArrayList<CoWeight>> retVal = null;
		try{			
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(cosetFile));
			retVal = (HashMap<Integer, ArrayList<CoWeight>>) in.readObject();
			in.close();
		}
		catch(Exception ex){
			// gulp
		}
		return retVal;
	}
	@SuppressWarnings("unchecked")
	private static HashMap<Integer, HashSet<Long>> gCorpDeser(String path){
		LOG.info("deser GCorp");
		HashMap<Integer, HashSet<Long>> retVal = null;
		try{			
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(path));
			retVal = (HashMap<Integer, HashSet<Long>>) in.readObject();
			in.close();
		}
		catch(Exception ex){
			// gulp
		}
		return retVal;
	}
}
