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

import com.twitter.corpus.types.Serialization2;

public class GlobalIndexGetDocCount {
	private static final Logger LOG = Logger.getLogger(GlobalIndexGetDocCount.class); 

	//
	//
	//
	//
	//		No longer necessary as tfMap has been created that is 150kb!!
	//
	//
	//
	//
	
	public static void main(String[] args) throws IOException{
		String path = "/trec/output/mRange3/nonIZeroGCorpDocCount.csv";
		String corpPath = "/trec/output/mRange3/trimGlobalIndex.ser";
		String output = "/trec/output/mRange3/docCounts.csv";
		
		HashSet<Integer> termSet =  (HashSet<Integer>) Serialization2.readFile(path);
		HashMap<Integer, HashSet<Long>> gCorp = Serialization2.deserialize(corpPath);
		
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
}
