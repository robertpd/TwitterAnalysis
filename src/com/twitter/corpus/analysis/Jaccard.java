package com.twitter.corpus.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.twitter.corpus.demo.IndexStatuses;
import com.twitter.corpus.types.CoWeight;

public class Jaccard {
	private static final Logger LOG = Logger.getLogger(Jaccard.class);
	public Jaccard(int size){
		jaccardList = new HashMap<Integer, ArrayList<Double>>(size);
	}
	private static int dayCounter = 0;
	public static HashMap<Integer, ArrayList<Double>> jaccardList;

	/*** 
	 * 
	 * @param feed array of cosets for "today" and "yesterday"
	 * @return maintains a static hashMap of jaccards for each term over duration of corpus, access when finished traversing the corpus
	 */
	public static void getJaccardSimilarity(ArrayList<HashMap<Integer, HashMap<Integer, Double>>> cosetArray){		
		// Get the union of the keys from both arrays
		ArrayList<HashMap<Integer, HashMap<Integer, Double>>> copyMap = new ArrayList<HashMap<Integer,HashMap<Integer, Double>>>(cosetArray);
		// TODO coset array contained somewhere 4370, think all entries are null and so size is zero... . why??
		// union set is set of terms(keys) that occur in both hashsets
		// incorrect: union set is set of outer keys, not correlates
		// need to get innet set of correlates to get the union
		Set<Integer> allTerms = new HashSet<Integer>(copyMap.get(0).keySet());
		// need intersection set to keep track of what terms have been checked
		// Set is good as it wont add a key already present
		// cant use simple int incremenet for intersection
		Set<Integer> interSet = null;
		Set<Integer> unionSet = null;
		allTerms.addAll(copyMap.get(1).keySet());
		copyMap = null;

		// now have all unique keys and can iterate
		Iterator<Integer> termIterator = allTerms.iterator();

		while(termIterator.hasNext()){
			Integer term = termIterator.next();
			try{
				interSet = new HashSet<Integer>(cosetArray.get(0).get(term).size());

				HashMap<Integer, Double> termCosetA = cosetArray.get(0).get(term);	// grab term correlates from both hashmaps
				HashMap<Integer, Double> termCosetB = cosetArray.get(1).get(term);

				int unionInit = 0;
				if( termCosetA != null ) { unionInit += termCosetA.size(); }
				if(termCosetB!=null){ unionInit += termCosetB.size(); }

				unionSet = new HashSet<Integer>(unionInit);
				if(termCosetA != null){ unionSet.addAll(termCosetA.keySet()); }
				if(termCosetB != null){	unionSet.addAll(termCosetB.keySet()); }

				// if term doesn't occur on particular day (unlightly, given thresholds performed already), termCoset will be null, this would give a 0 jaccard
				if(termCosetB != null && termCosetA != null){
					// iterate A, get coweight and check i
					Iterator<Integer> aIter = termCosetA.keySet().iterator();
					while(aIter.hasNext()){
						Integer t = aIter.next();
						if(termCosetB.containsKey(t)){
							interSet.add(t);
						}
					}
					termCosetB.keySet().removeAll(interSet);	// remove all keys that have already been detected
					Iterator<Integer> iterB = termCosetB.keySet().iterator();
					while(iterB.hasNext()){
						Integer t = iterB.next();
						if(termCosetA.containsKey(t)){
							interSet.add(t);
						}
					}
					double jaccard = 0.0;
					if(interSet.size() > 0 && unionSet.size() > 0){
						jaccard = (double)interSet.size() / (double) unionSet.size();	// switch to union
					}

					if(jaccard > 0){
						if(!jaccardList.containsKey(term)){
							ArrayList<Double> jEachDay = new ArrayList<Double>(33);
							jEachDay.add(dayCounter, jaccard);
							jaccardList.put(term, jEachDay);
						}
						else{
							jaccardList.get(term).add(dayCounter, jaccard);
						}
					}
				}
				else{

				}
			}
			catch(NullPointerException np){
				LOG.info("NullPointerException" + np);
			}
		}
		dayCounter++; // must be incremented here to ensure continuity
	}
}