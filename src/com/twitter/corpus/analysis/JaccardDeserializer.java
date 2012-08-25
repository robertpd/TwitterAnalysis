package com.twitter.corpus.analysis;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class JaccardDeserializer {

	public static void main(String[] args) throws IOException{
		String path = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.15/";
		String file = "jaccardNon_Weighted";
		String type = ".ser";
		HashMap<Integer, HashMap<Integer,Double>> jaccard = deserialize(path + file + type);
		
		// get avg vol, std dev
		VolatilityAnalysis va = new VolatilityAnalysis();
		va.avgVolatility(jaccard, (path + file));
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