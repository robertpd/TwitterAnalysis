package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.CoWeight;

public class CosetDeserTermToString {
public static final Logger LOG = Logger.getLogger(CosetDeserTermToString.class);
	public static void main(String[] args) throws IOException{
		//		String input = "/home/dock/Documents/IR/AmazonResults/";
		//		String output = "/home/dock/Documents/IR/AmazonResults/termIdTranslation";

		String input = "/analysis/output/";
		String output = "/analysis/output/termTranslation";

		String root = "termCoset_";
		String base = ".ser";

		// input is text file

		HashSet<Integer> terms = getTermIds(input + "termTranslation/" + "terms.txt");
		HashMap<Integer, ArrayList<CoWeight>> termSet = new HashMap<Integer, ArrayList<CoWeight>>(terms.size());

		// input is termBimap

		LOG.info("starting termbimap deserialization");
		HashBiMap<String, Integer> termBimap = deserTermBiMap(input + "/termbimap.ser");
		LOG.info("finished termbimap deserialization\n termBimap size is: " + termBimap.size());

		String[] filePaths = new String[33];

		// input is termcosets

		for(int i = 0; i < 33 ; i++){
			filePaths[i] = input + "termTranslation/termcoset/" + root + (i+1) + base;
		}

		for(String path : filePaths){

			String cosetPath = path;
			HashMap<Integer, ArrayList<CoWeight>> coset = deser(cosetPath);

			Iterator<Integer> iter = terms.iterator();
			while(iter.hasNext()){
				Integer t = iter.next();

				ArrayList<CoWeight> entry = null;
				if(coset.get(t) != null){
					entry = coset.get(t);
					termSet.put(t, entry);
				}
			}
		}
		LOG.info("termSet size is : " + termSet.size());

		BufferedWriter bf =  new BufferedWriter(new FileWriter(output + "/termStrings.txt"));

		Iterator<Entry<Integer, ArrayList<CoWeight>>> cwIter = termSet.entrySet().iterator();
		while(cwIter.hasNext()){
			Entry<Integer, ArrayList<CoWeight>> entry = cwIter.next();
			Integer term = entry.getKey();

			if(termBimap.inverse().get(term) != null){
				LOG.info(termBimap.inverse().get(term));
				
				
				bf.append(termBimap.inverse().get(term) + ": ");

				Iterator<CoWeight> setIter = entry.getValue().iterator();
				while(setIter.hasNext()){
					CoWeight cw = setIter.next();
					bf.append(termBimap.inverse().get(cw.termId) + ", ");					
				}
				bf.append("\n");
			}
			else{
				LOG.info(term + " gives null pointer. set size is: " +termSet.size()+  "\n");
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static HashMap<Integer, ArrayList<CoWeight>> deser(String path){
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
	private static HashBiMap<String, Integer> deserTermBiMap(String path){
		File bimapFile = new File(path);

		HashBiMap<String, Integer> retVal = null;
		try{			
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(bimapFile));
			retVal = (HashBiMap<String, Integer>) in.readObject();
			in.close();
		}
		catch(Exception ex){
			// gulp
		}
		return retVal;
	}
	private static HashSet<Integer> getTermIds(String path) throws IOException{
		Set<String> termIdString = new LinkedHashSet<String>();
		FileInputStream stream = new FileInputStream(new File(path));
		HashSet<Integer> termIdInts = null;
		try {
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
			/* Instead of using default, pass in a decoder. */
			String x = Charset.defaultCharset().decode(bb).toString().replace(",", " ");
			String y = x.replace("\n", " ");
			String[] a = y.split(" ");

			for(int i=0; i<a.length;i++){
				termIdString.add(a[i].replaceAll("[^0-9]",""));
			}
			termIdInts = new HashSet<Integer>(termIdString.size());
			Iterator<String> iter = termIdString.iterator();
			while(iter.hasNext()){
				termIdInts.add(Integer.parseInt(iter.next()));
			}
		}
		finally {
			stream.close();
		}
		return termIdInts;
	}
}