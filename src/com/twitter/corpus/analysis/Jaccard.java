package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.twitter.corpus.demo.TweetAnalysis;
import com.twitter.corpus.types.CoWeight;

public class Jaccard {
	private static final Logger LOG = Logger.getLogger(Jaccard.class);
	public Jaccard(int size){
		jaccardList = new HashMap<Integer, HashMap<Integer,Double>>(size);
	}
	// dayCounter is used for jaccard map, also printed out on entry to jaccard calculator
	private static int dayCounter = 0;
	private static int unionZero = 0;
	private static int termError = 0;
	public static HashMap<Integer, HashMap<Integer,Double>> jaccardList;

	/***
	 *
	 * @param feed array of cosets for "today" and "yesterday"
	 * @return maintains a static hashMap of jaccards for each term over duration of corpus, access when finished traversing the corpus
	 * @throws IOException
	 */
	public static void getJaccardSimilarity(ArrayList<HashMap<Integer, HashMap<Integer, Double>>> cosetArray) throws IOException{
		LOG.info("Interval: " + (dayCounter+1));

		// Get the list of all terms

		ArrayList<HashMap<Integer, HashMap<Integer, Double>>> copyMap = new ArrayList<HashMap<Integer,HashMap<Integer, Double>>>(cosetArray);
		// TODO coset array contained somewhere 4370, think all entries are null and so size is zero... . why??
		Set<Integer> allTerms = new HashSet<Integer>(copyMap.get(0).keySet());
		allTerms.addAll(copyMap.get(1).keySet());
		copyMap = null;

		// iterate all terms

		Iterator<Integer> termIterator = allTerms.iterator();
		while(termIterator.hasNext()){
			Integer term = termIterator.next();

			// need to re-arch to take only top 5 coweights
			// try{
			// interSet = new HashSet<Integer>(cosetArray.get(0).get(term).size());

			// cutoff is init size for the trimmed

			int cutoff = 5;
			HashSet<Integer> a = null;
			HashSet<Integer> b = null;

			// try get coweight set for term from both days

			try{
				a = sortList(cosetArray.get(0).get(term), cutoff);
			}
			catch (NullPointerException e) {
				// gulp!
			}
			try{
				b = sortList(cosetArray.get(1).get(term), cutoff);
			}
			catch (NullPointerException e) {
				// gulp!
			}

			// set default jaccard to 0.0

			Double jac = 0.0;

			// if either a or b is null, then jaccard is just z. 0 is kept in the loop to provide continuity

			if(a != null && b != null){	// if either are null jac == 0.0 will be added

				// init intersection and union sets

				HashSet<Integer> inters = new HashSet<Integer>(cutoff);
				// HashSet<Integer> unions = new HashSet<Integer>(cutoff * 2);

				// iterate one coweight set for intersection

				Iterator<Integer> aIter2 = a.iterator();
				while(aIter2.hasNext()){
					Integer a2 = aIter2.next();
					if(b.contains(a2)){
						inters.add(a2);
					}
				}

				// get union set

				a.addAll(b);

				// unions.addAll(a);
				// unions.addAll(b);

				// get jaccard, dont bother if union is 0

				if(a.size() != 0){
					jac = (double)Math.round(((double)inters.size() / (double)a.size()) * 1000) / 1000;
				}
			}

			// add jaccard value for the interval, if first time add term first

			if(!jaccardList.containsKey(term)){

				// init map of jaccard to interval and add j

				HashMap<Integer, Double> jEachDayMap = new HashMap<Integer, Double>(32);	// 17 + 16 = 33 days => 32 intervals

				// scratch that - pre init this map with 9.9 so we have an entry for every interval and we can distinguish the 9.9 as interval in which term was missing on a particular day!

				// pre init this map with 0.0

				for(int i =0 ; i < 32 ;i++){
					jEachDayMap.put(i, 0.0);
				}
				jEachDayMap.put(dayCounter, jac);

				// add mapping for term
				jaccardList.put(term, jEachDayMap);
			}
			else{
				jaccardList.get(term).put(dayCounter, jac);
			}

		}
		LOG.info("Jacard size = " + jaccardList.size());
		// LOG.info("Term error occured " + termError + "times." );
		dayCounter++; // must be incremented here to ensure continuity
	}

	/*** 
	 * 
	 * @param feed array of cosets for "today" and "yesterday"
	 * @return maintains a static hashMap of jaccards for each term over duration of corpus, access when finished traversing the corpus
	 * @throws IOException 
	 */
	public static void getJaccardEnhancedSimilarity(ArrayList<HashMap<Integer, ArrayList<CoWeight>>> cosetArray) throws IOException{
		LOG.info("Interval: " + (dayCounter+1));

		// Get the list of all terms 

		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> copyMap = new ArrayList<HashMap<Integer, ArrayList<CoWeight>>>(cosetArray);
		// TODO coset array contained somewhere 4370, think all entries are null and so size is zero... . why??
		Set<Integer> allTerms = new HashSet<Integer>(copyMap.get(0).keySet());
		allTerms.addAll(copyMap.get(1).keySet());
		copyMap = null;

		// iterate all terms

		Iterator<Integer> termIterator = allTerms.iterator();
		while(termIterator.hasNext()){
			Integer term = termIterator.next();

			int cutoff = 5;
			ArrayList<CoWeight> a = null;
			ArrayList<CoWeight> b = null;

			
			try{
				if(cosetArray.get(0).get(term).size() >= 5){
					a = new ArrayList<CoWeight>(cosetArray.get(0).get(term).subList(0, cutoff - 1));
				}
				else{
					a = new ArrayList<CoWeight>(cosetArray.get(0).get(term));
				}
			}
			catch (NullPointerException e) {
				// gulp!
			}
			try{
				if(cosetArray.get(1).get(term).size() >= 5){
					b = new ArrayList<CoWeight>(cosetArray.get(1).get(term).subList(0, cutoff - 1));
				}
				else{
					b = new ArrayList<CoWeight>(cosetArray.get(1).get(term));
				}
			}
			catch (NullPointerException e) {
				//  gulp!
			}

			// if either a or b is null, then jaccard is just z. 0 is kept in the loop to provide continuity			
			Double jac = 0.0;
			if(a != null && b != null){	// if either are null jac == 0.0 will be added
				jac = getJaccardVal(a,b);
			}

			// add jaccard value for the interval, if first time add term first

			if(!jaccardList.containsKey(term)){

				// init map of jaccard to interval and add j
				HashMap<Integer, Double> jEachDayMap = new HashMap<Integer, Double>(32);	// 17 + 16 = 33 days => 32 intervals
				// pre init this map with 0.0
				for(int i =0 ; i < 32 ;i++){
					jEachDayMap.put(i, 0.0);
				}

				jEachDayMap.put(dayCounter, jac);

				// add mapping for term
				jaccardList.put(term, jEachDayMap);
			}
			else{
				jaccardList.get(term).put(dayCounter, jac);
			}

		}
		LOG.info("Jacard size = " + jaccardList.size());
		//		LOG.info("Term error occured " + termError + "times." );
		dayCounter++; // must be incremented here to ensure continuity
	}
	private static Double getJaccardVal(ArrayList<CoWeight> a, ArrayList<CoWeight> b){

		// convert arraylist to hashmap to make coweight access easy

		HashMap<Integer, Double> aMap = new HashMap<Integer, Double>(a.size());
		Iterator<CoWeight> aIter = a.iterator();
		while(aIter.hasNext()){
			CoWeight acw = aIter.next();
			aMap.put(acw.termId, acw.correlate);
		}
		HashMap<Integer, Double> bMap = new HashMap<Integer, Double>(a.size());
		Iterator<CoWeight> bIter = b.iterator();
		while(bIter.hasNext()){
			CoWeight bcw = bIter.next();
			bMap.put(bcw.termId, bcw.correlate);
		}

		// iterate aMap and crosscheck

		Double jacc = 0.0;
		HashSet<Integer> termsDone = new HashSet<Integer>();
		Iterator<Entry<Integer, Double>> aMapIter = aMap.entrySet().iterator();
		while(aMapIter.hasNext()){
			Entry<Integer, Double> entry = aMapIter.next();
			Integer aTerm = entry.getKey();
			Double aWeight = entry.getValue();
			if(bMap.containsKey(entry.getKey())){
				// square the value
				jacc += Math.pow( Math.abs(bMap.get(entry.getKey()) - aWeight), 2);
				termsDone.add(aTerm);
			}
			else{
				// dont add term from here to termsDone as, from the b persepective we check if a has terms we know b has, and thus they are already done
				jacc += Math.pow( aWeight, 2 );
			}	
		}

		// iterate bMap for terms not contained in A

		Iterator<Entry<Integer, Double>> bMapIter = bMap.entrySet().iterator();
		while(bMapIter.hasNext()){
			Entry<Integer, Double> entry = bMapIter.next();
			Integer bTerm = entry.getKey();
			if(!termsDone.contains(bTerm)){
				Double bWeight = entry.getValue();
				jacc += Math.pow(bWeight, 2);
			}
		}

		// et voila!

		return jacc;
	}
	/***
	 * Calculate jaccard differences across intervals.
	 *  @return HashMap of term and jaccard differences for each interval
	 */
	public static HashMap<Integer, ArrayList<Double>> calcJaccardDifferences(HashMap<Integer, HashMap<Integer,Double>> jaccardList){	// previous prototype was empty and jaccardList below was the static object from Jaccard class..
		LOG.info("Calc'ing jaccard differences");

		// retVal hashmap is init to size of jaccard list, jaccardList is a hashmap

		HashMap<Integer , ArrayList<Double>> retVal = new HashMap<Integer, ArrayList<Double>>(jaccardList.size());
		Iterator<Entry<Integer, HashMap<Integer, Double>>> jaccardIter = jaccardList.entrySet().iterator();

		while(jaccardIter.hasNext()){
			Entry<Integer, HashMap<Integer, Double>> termJSet = jaccardIter.next();
			HashMap<Integer, Double> termJIntervals = termJSet.getValue();
			Integer outerTerm = termJSet.getKey();
			Iterator<Entry<Integer, Double>> jaccardSetIter = termJIntervals.entrySet().iterator();
			Double last = 0.0;
			boolean firstIter = false;

			while(jaccardSetIter.hasNext()){

				Entry<Integer, Double> jVal = jaccardSetIter.next();
				Double value = jVal.getValue();
				Integer interval = jVal.getKey();
				if(!firstIter){	// exec once to buffer 1st jaccard value
					firstIter = true;
					last = value;
					continue;
				}

				Double jd = (double)Math.round(((double)(value - last)) * 1000) / 1000;
				//				Double jd = 2.1;
				if(!retVal.containsKey(termJSet.getKey())){
					ArrayList<Double> intervalDiffs = new ArrayList<Double>();
					int capacity = 31;
					intervalDiffs.ensureCapacity(capacity);
					for( int i =0; i<capacity ;i++){
						intervalDiffs.add(i, 0.0);
					}
					// can we be sure that the intervalDiffs array will be filled from 0?? YES
					intervalDiffs.set((interval - 1) , jd);
					// add jaccard difference for a term
					retVal.put(outerTerm, intervalDiffs);
				}
				else{
					// get a terms jaccard difference set
					retVal.get(outerTerm).set(jVal.getKey()-1, jd);
				}

				last = value;
			}
		}
		LOG.info("Finished diffing jaccards");
		return retVal;
	} 

	/***
	 * Sort the coset hashmap of term and weights by weight. Return a Set of the top "cutoff" terms.
	 * @param termCoset
	 * @param cutoff
	 * @return
	 */
	public static HashSet<Integer> sortList(HashMap<Integer, Double> termCoset, int cutoff){

		ArrayList<Entry<Integer, Double>> top5Aranked = new ArrayList<Entry<Integer, Double>>(termCoset.size());

		// add all from hashmap to arraylist so they can be sorted
		Iterator<Entry<Integer, Double>> coweightAIter = termCoset.entrySet().iterator();
		while(coweightAIter.hasNext()){
			top5Aranked.add(coweightAIter.next());
		}

		// sort by weight
		Collections.sort(top5Aranked, new top5Comp());

		// for new jaccard method need term and weight returned so ...
		//	return top5Aranked;

		// old jaccard used a return hashSet - no sacrifice in efficiency with new return type

		// init sorted top 5 list
		ArrayList<Integer> sortedTop5A = new ArrayList<Integer>(cutoff);
		//		
		//		// get and add top 5 from already sorted list
		//		
		Iterator<Entry<Integer, Double>> liter = top5Aranked.iterator();		
		int breakCounter = 0;		
		while(liter.hasNext() && breakCounter < cutoff){
			// i can imagine that future enhancements will require the coweight type to keep term and weight together
			sortedTop5A.add(liter.next().getKey());
			breakCounter++;
		}
		//		
		//		// convert to hashset
		//		
		HashSet<Integer> retVal = new HashSet<Integer>(sortedTop5A);

		return retVal;
	}
	/***
	 * Serialize the jaccard differences to avoid doing all that work again!
	 * @param jDiffs
	 * @throws IOException 
	 */
	public static void serializeJDiff(HashMap<Integer, ArrayList<Double>> jDiffs, String outputPath) throws IOException{
		String path = outputPath + "/jDif.ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(jDiffs);
		objectOut.close();
	}
	public static void serializeJaccards(String outputPath) throws IOException{
		LOG.info("Serializing Jaccards.");
		String path = outputPath + "/jaccard.ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(jaccardList);
		objectOut.close();
		LOG.info("Finished serializing Jaccards.");
	}
	public static void printResults() throws IOException{
		BufferedWriter out = new BufferedWriter(new FileWriter(TweetAnalysis.output));
		Iterator<Entry<Integer, HashMap<Integer, Double>>> termIter = jaccardList.entrySet().iterator();
		while(termIter.hasNext()){
			Entry<Integer, HashMap<Integer, Double>> termJacc = termIter.next();
			StringBuffer sb = new StringBuffer();
			sb.append(TermTermWeights.termBimap.inverse().get(termJacc.getKey()) + " { ");

			Iterator<Entry<Integer, Double>> jaccIter = termJacc.getValue().entrySet().iterator();
			while(jaccIter.hasNext()){
				Entry<Integer, Double> entry = jaccIter.next();
				double val = (double)Math.round(entry.getValue() * 1000) / 1000;
				sb.append(entry.getKey().toString() + " = " + val + ", ");
			}
			out.write(sb.replace(sb.length()-2, sb.length()-2," }\n").toString());
		}
		out.close();
	}
	// implement for descending rank
	public static class top5Comp implements Comparator<Entry<Integer, Double>> {
		@Override
		public int compare(Entry<Integer, Double> c1, Entry<Integer, Double> c2) {
			return -c1.getValue().compareTo(c2.getValue());
		}
	}
}