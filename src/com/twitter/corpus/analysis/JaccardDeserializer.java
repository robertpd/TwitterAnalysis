package com.twitter.corpus.analysis;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;

public class JaccardDeserializer {

	public static void main(String[] args){

		ArrayList<Integer> poop = new ArrayList<Integer>(10);
		poop.add(0, 1);
		String file = args[0].toString() ;
		HashMap<Integer, HashMap<Integer,Double>> jaccard = deserialize(file);
		HashMap<Integer, ArrayList<Double>> jacDif = Jaccard.calcJaccardDifferences(jaccard);
	}

	@SuppressWarnings("unchecked")
	private static HashMap<Integer, HashMap<Integer,Double>> deserialize(String file){
		File cosetFile = new File(file);
		HashMap<Integer, HashMap<Integer,Double>> retVal = null;
		try{			
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(cosetFile));
			retVal = (HashMap<Integer, HashMap<Integer,Double>>) in.readObject();
			in.close();
		}
		catch(Exception ex){
			// gulp
		}
		return retVal;
	}
}