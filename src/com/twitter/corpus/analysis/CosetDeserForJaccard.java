package com.twitter.corpus.analysis;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import com.twitter.corpus.types.CoWeight;


public class CosetDeserForJaccard {
	public static void main(String[] args) throws IOException{
//		String input = args[0].toString() ;
//		String output = args[1].toString();
		
		String input = "/home/dock/Documents/IR/AmazonResults/StemmedResults1/";
		String output = "/home/dock/Documents/IR/AmazonResults/StemmedResults1";
//		String head = "/home/dock/Documents/IR/AmazonResults/total/coset/";
		
		String root = "termCoset_";
		String base = ".ser";

		String[] filePaths = new String[33];

		for(int i = 0; i < 33 ; i++){
			filePaths[i] = input + root + (i+1) + base;
		}
		
		Jaccard initJMap = null;
		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corpusCoSetArray = new ArrayList<HashMap<Integer, ArrayList<CoWeight>>>(2);
		for(String path : filePaths){
			//			String head = "/analysis/output/";
			
			String cosetPath = path;
			HashMap<Integer, ArrayList<CoWeight>> coset = deser(cosetPath);
			corpusCoSetArray.add(coset);
			
			if(corpusCoSetArray.size() == 2){	// only skipped once at the start
				if(initJMap == null){			// one time initializer
					initJMap = new Jaccard(20000);	// init size plus 10% for wiggle
				}
				// 3.0 do the deed
				int cutoff = 5;
				Jaccard.getJaccardSimilarity(corpusCoSetArray, cutoff);
//				Jaccard.getJaccardEnhancedSimilarity(corpusCoSetArray, cutoff);

				// swap positions, makes our life easier
				Collections.swap(corpusCoSetArray, 0, 1);
				// remove the first coset array
				corpusCoSetArray.remove(1);
			}
		}
		Jaccard.serializeJaccards(output);
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
}
