package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.log4j.Logger;

public class GlobalIndexGetDocCount {
	private static final Logger LOG = Logger.getLogger(GlobalIndexGetDocCount.class); 

	public static void main(String[] args) throws IOException{
		String path = "/trec/output/mRange3/nonIZeroGCorpDocCount.csv";
		String corpPath = "/trec/output/mRange3/trimGlobalIndex.ser";
		String output = "/trec/output/mRange3/docCounts.csv";
		
		HashSet<Integer> termSet =  (HashSet<Integer>) TermBimapDeser.readFile(path);
		HashMap<Integer, HashSet<Long>> gCorp = gCorpDeser(corpPath);
		
		ArrayList<Integer> docCounts = new ArrayList<Integer>();
		for(Integer term : termSet){
			docCounts.add(gCorp.get(term).size());
		}
		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
		Iterator<Integer> docIter = docCounts.iterator();
		while(docIter.hasNext()){
			bw.append(docIter.next().toString() + "\n");
		}
		bw.close();
		LOG.info("file written");
	}
	private static HashMap<Integer, HashSet<Long>> gCorpDeser(String path){
		LOG.info("deser global corp");
		File cosetFile = new File(path);

		HashMap<Integer, HashSet<Long>> retVal = null;
		try{			
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(path));
			retVal = (HashMap<Integer, HashSet<Long>>) in.readObject();
			in.close();
		}
		catch(Exception ex){
			// gulp
		}
		LOG.info("global corp deserialized.");
		return retVal;
	}
}
