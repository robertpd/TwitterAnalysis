package com.twitter.corpus.analysis;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class JaccardDeserializer {

	private static final int jDiffThreshold = 10;	// max zero entries, zero could mean zero diff or be propagated from zero jaccard
	
	public static void main(String[] args){

		ArrayList<Integer> poop = new ArrayList<Integer>(10);
		poop.add(0, 1);
		String file = args[0].toString() ;
		HashMap<Integer, HashMap<Integer,Double>> jaccard = deserialize(file);
		HashMap<Integer, ArrayList<Double>> jacDif = Jaccard.calcJaccardDifferences(jaccard);
		HashMap<Integer, ArrayList<Double>> jDiffClean = scrubJDiff(jacDif, jDiffThreshold);
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
	/***
	 * Remove entries with greater than threshold zeros.
	 * @param jDiff, threshold
	 * @return
	 */
	private static HashMap<Integer, ArrayList<Double>> scrubJDiff(HashMap<Integer, ArrayList<Double>> jDiff, int threshold){
		
		HashMap<Integer, ArrayList<Double>> retVal = new HashMap<Integer, ArrayList<Double>>(jDiff.size());
		
		// iterate jDiff
		
		Iterator<Entry<Integer, ArrayList<Double>>> jDiffIter = jDiff.entrySet().iterator();
		while(jDiffIter.hasNext()){
			Entry<Integer, ArrayList<Double>> entry = jDiffIter.next();
			Integer key = entry.getKey();
			ArrayList<Double> jDiffSet = entry.getValue();
			
			int zeroCounter = 0;
			for(Double val : jDiffSet){
				if(val == 0.0){
					zeroCounter++;
				}
			}
			if(zeroCounter < threshold){
				retVal.put(key, jDiffSet);
			}
		}		
		return retVal;		
	}
}