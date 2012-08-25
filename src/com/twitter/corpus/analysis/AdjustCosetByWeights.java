package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.twitter.corpus.types.CoWeight;

public class AdjustCosetByWeights {
	private static final Logger LOG = Logger.getLogger(AdjustCosetByWeights.class);
	public static void main(String[] args) throws IOException{

		Double m = 0.17;

		String input = "/home/dock/Documents/IR/AmazonResults/mRange3/tc/";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.17/";
		String root = "termCoset_";
		String base = ".ser";

		String[] filePaths = new String[33];
		for(int i = 0; i < 33 ; i++){
			filePaths[i] = input + root + (i+1) + base;
		}

		Integer termCosetCounter = 1;

		for(String path : filePaths){
			String cosetPath = path;
			HashMap<Integer, ArrayList<CoWeight>> coset = CosetDeserForJaccard.deserialize(cosetPath);
			HashMap<Integer, ArrayList<CoWeight>> newCosetMap = new HashMap<Integer, ArrayList<CoWeight>>(coset.size());

			Iterator<Map.Entry<Integer, ArrayList<CoWeight>>> iterOldCoset = coset.entrySet().iterator();
			while(iterOldCoset.hasNext()){
				Entry<Integer, ArrayList<CoWeight>> entry = iterOldCoset.next();
				Integer term = entry.getKey();
				ArrayList<CoWeight> oldCoset = entry.getValue();

				ArrayList<CoWeight> newCoset = new ArrayList<CoWeight>();
				Iterator<CoWeight> cowIter = oldCoset.iterator();
				while(cowIter.hasNext()	){
					CoWeight cw = cowIter.next();
					if(cw.correlate >= m){
						newCoset.add(cw);
					}
				}
				if(newCoset.size() > 0){
					newCosetMap.put(term, newCoset);
				}
			}
			CosetSerializer.cosetSerializer(newCosetMap, output, termCosetCounter);
			termCosetCounter++;
		}
		LOG.info("Finished trimming Cosets.");
	}
}