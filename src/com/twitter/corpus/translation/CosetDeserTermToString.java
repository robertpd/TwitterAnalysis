package com.twitter.corpus.translation;

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
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.CoWeight;

/**
 * 
 * Provide translation of cosets
 *
 */
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
		HashMap<Integer, ArrayList<CoWeight>> termsToTranslate = new HashMap<Integer, ArrayList<CoWeight>>(terms.size());

		// input is termBimap

		LOG.info("starting termbimap deserialization");
		HashBiMap<String, Integer> termBimap = deserTermBiMap(input + "/termbimap.ser");
		LOG.info("finished termbimap deserialization\n termBimap size is: " + termBimap.size());

		// filepaths to term cosets
		String[] filePaths = new String[33];

		// input is termcosets
		for(int i = 0; i < 33 ; i++){
			filePaths[i] = input + "termTranslation/termcoset/" + root + (i+1) + base;
		}

		// filepaths to each term coset
		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> termCosetArraylist = new ArrayList<HashMap<Integer, ArrayList<CoWeight>>>();

		for(String path : filePaths){
			HashMap<Integer, ArrayList<CoWeight>> coset = deser(path);
			termCosetArraylist.add(coset);
		}
		LOG.info("termSet size is : " + termsToTranslate.size());

		BufferedWriter bf =  new BufferedWriter(new FileWriter(output + "/termStrings.txt"));

		// iterate my list of terms to translate
		Iterator<Integer> termsIter = terms.iterator();
		while(termsIter.hasNext()){
			// get the term
			Integer t = termsIter.next();

			// iterate term cosets
			Iterator<HashMap<Integer, ArrayList<CoWeight>>> cosetIter = termCosetArraylist.iterator();
			while(cosetIter.hasNext()){
				HashMap<Integer, ArrayList<CoWeight>> coset = cosetIter.next();
				ArrayList<CoWeight> termSet = coset.get(t);

				bf.append(termBimap.inverse().get(t) + ": ");

				Iterator<CoWeight> cwIter = termSet.iterator();
				while(cwIter.hasNext()){
					CoWeight cw = cwIter.next();
					bf.append(termBimap.inverse().get(cw.termId) + ", ");
				}
				bf.append("\n");
			}
		}
		bf.close();	}

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