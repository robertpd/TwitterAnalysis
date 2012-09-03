package com.twitter.corpus.analysis;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import com.twitter.corpus.types.CoWeight;
import com.twitter.corpus.types.Serialization2;


public class CosetDeserForJaccard {
	public static void main(String[] args) throws IOException{

		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.17/";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.17/";
		String root = "termCoset_";
		String base = ".ser";

		String[] filePaths = new String[33];

		for(int i = 0; i < 33 ; i++){
			filePaths[i] = input + root + (i+1) + base;
		}
		
		Jaccard jaccardSim = null;
		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corpusCoSetArray = new ArrayList<HashMap<Integer, ArrayList<CoWeight>>>(2);
		
		for(String path : filePaths){
			String cosetPath = path;
			HashMap<Integer, ArrayList<CoWeight>> coset = Serialization2.deserialize(cosetPath);
			corpusCoSetArray.add(coset);
			
			if(corpusCoSetArray.size() == 2){
				if(jaccardSim == null){
					jaccardSim = new Jaccard(20000);
				}
				// 3.0 do the deed
				int cutoff = 20;
				jaccardSim.getJaccardSimilarity(corpusCoSetArray, cutoff);
				jaccardSim.getJaccardWeightedSimilarity(corpusCoSetArray, cutoff);
				
				// swap positions, makes our life easier
				Collections.swap(corpusCoSetArray, 0, 1);
				// remove the first coset array
				corpusCoSetArray.remove(1);
			}
		}
		Jaccard.serializeJaccards(output);
	}
}