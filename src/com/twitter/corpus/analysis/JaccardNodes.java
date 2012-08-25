package com.twitter.corpus.analysis;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import com.twitter.corpus.types.CoWeight;

public class JaccardNodes {
	public static void main(String[] args) throws IOException{

		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.17/";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.17/";
		String root = "termCoset_";
		String base = ".ser";

		String[] filePaths = new String[33];

		for(int i = 0; i < 33 ; i++){
			filePaths[i] = input + root + (i+1) + base;
		}

		Jaccard jaccardNodes = null;
		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corpusCoSetArray = new ArrayList<HashMap<Integer, ArrayList<CoWeight>>>(2);

		for(String path : filePaths){
			String cosetPath = path;
			HashMap<Integer, ArrayList<CoWeight>> coset = deserialize(cosetPath);
			corpusCoSetArray.add(coset);

			if(corpusCoSetArray.size() == 2){
				if(jaccardNodes == null){
					jaccardNodes = new Jaccard(20000);
				}
				// 3.0 do the deed
				int cutoff = 20;
				jaccardNodes.getJaccardSimilarity(corpusCoSetArray, cutoff);
				jaccardNodes.getJaccardWeightedSimilarity(corpusCoSetArray, cutoff);

				// swap positions, makes our life easier
				Collections.swap(corpusCoSetArray, 0, 1);
				// remove the first coset array
				corpusCoSetArray.remove(1);
			}
		}
		Jaccard.serializeJaccards(output);
	}
	private HashMap<>
	@SuppressWarnings("unchecked")
	public static HashMap<Integer, ArrayList<CoWeight>> deserialize(String path){
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
}