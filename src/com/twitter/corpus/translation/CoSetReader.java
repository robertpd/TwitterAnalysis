package com.twitter.corpus.translation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.CoWeight;

/**
 * Read cosets and translate integer terms to string
 *
 */
public class CoSetReader {
	private static HashMap<Integer, ArrayList<CoWeight>> coSetMapArray;// = new HashMap<Integer, ArrayList<CoWeight>>();
	@SuppressWarnings("unchecked")
	public static void main(String[] args){

		File cosetFile = new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/coset11k.ser");
		try{
			ObjectInputStream docTermMapois = new ObjectInputStream(new FileInputStream(cosetFile));
			coSetMapArray = (HashMap<Integer, ArrayList<CoWeight>>) docTermMapois.readObject();
			docTermMapois.close();
		}
		catch(Exception ex){}
		
		File termBiMapFile = new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/termBiMap11k.ser");
		HashBiMap<String,Integer> termBimap;// = HashBiMap.create();
		try{
			ObjectInputStream docTermMapois = new ObjectInputStream(new FileInputStream(termBiMapFile));
			termBimap = (HashBiMap<String,Integer>) docTermMapois.readObject();
			docTermMapois.close();
		
		BufferedWriter out = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/coset11k_0_3.txt"));
		Set<Integer> keyset = coSetMapArray.keySet();
		if((coSetMapArray != null )&& (termBimap != null)){
			for(Integer c : keyset){
				ArrayList<CoWeight> cwList = coSetMapArray.get(c);
				StringBuffer sb = new StringBuffer();
				sb.append(termBimap.inverse().get(c)  + " { ");
				boolean createLine=false;
				for(CoWeight cw: cwList){
					if(cw.correlate > 0.3){
						createLine = true;
						sb.append(termBimap.inverse().get(cw.termId)+ ": " + cw.correlate + ", ");						
					}
				}
				if(createLine){
					out.write(sb.append("\n").toString());
				}
			}
		}
	}catch(Exception ex){
		System.out.print(ex.getMessage());
	}
	}
}
