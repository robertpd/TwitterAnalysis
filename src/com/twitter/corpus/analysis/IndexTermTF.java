package com.twitter.corpus.analysis;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public class IndexTermTF {
	private static final Logger LOG = Logger.getLogger(IndexTermTF.class);
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException{
		
		String corpPath = "/trec/output/mRange3/trimGlobalIndex.ser";
		String tfMapPath = "/trec/output/mRange3/tfMap.ser";
		
		LOG.info("De-serializing corpus index.");
		HashMap<Integer, HashSet<Long>> retVal = null;
		try{			
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(corpPath));
			retVal = (HashMap<Integer, HashSet<Long>>) in.readObject();
			in.close();
		}
		catch(Exception ex){
			// gulp
		}
		HashMap<Integer, Integer> tfMap = new HashMap<Integer, Integer>(retVal.size());
		
		Iterator<Entry<Integer, HashSet<Long>>> corpIter = retVal.entrySet().iterator();
		while(corpIter.hasNext()){
			Entry<Integer, HashSet<Long>> entry = corpIter.next();
			tfMap.put(entry.getKey(), entry.getValue().size());
		}
		
		LOG.info("Serializing tf map.");
		FileOutputStream fileOut = new FileOutputStream(tfMapPath);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(tfMap);
		objectOut.close();
		LOG.info("Finished serializing tf map.");
	}
}