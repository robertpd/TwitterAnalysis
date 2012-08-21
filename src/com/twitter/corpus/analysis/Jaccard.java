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

import com.twitter.corpus.types.CoWeight;

public class Jaccard {
	private static final Logger LOG = Logger.getLogger(Jaccard.class);
	public Jaccard(int size){
		jaccardListNonWeighted = new HashMap<Integer, HashMap<Integer,Double>>(size);
		jaccardListWeighted = new HashMap<Integer, HashMap<Integer,Double>>(size);
	}
	// dayCounter is used for jaccard map, also printed out on entry to jaccard calculator
	private static int dayCounter = 0;
	private static int dayCounterWeighted = 0;
	private static int dayCounterNonWeighted = 0;
	private static int unionZero = 0;
	private static int termError = 0;
	public static HashMap<Integer, HashMap<Integer,Double>> jaccardListNonWeighted;
	public static HashMap<Integer, HashMap<Integer,Double>> jaccardListWeighted;

	/***
	 *
	 * @param feed array of cosets for "today" and "yesterday"
	 * @return maintains a static hashMap of jaccards for each term over duration of corpus, access when finished traversing the corpus
	 * @throws IOException
	 */
	public void getJaccardSimilarity(ArrayList<HashMap<Integer, ArrayList<CoWeight>>> cosetArray, int cutoff) throws IOException{
		LOG.info("Interval: " + (dayCounterNonWeighted+1));

		// Get the list of all terms

		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> copyMap = new ArrayList<HashMap<Integer,ArrayList<CoWeight>>>(cosetArray);
		// TODO coset array contained somewhere 4370, think all entries are null and so size is zero... . why??
		Set<Integer> allTerms = new HashSet<Integer>(copyMap.get(0).keySet());
		allTerms.addAll(copyMap.get(1).keySet());
		copyMap = null;

		// iterate all terms

		Iterator<Integer> termIterator = allTerms.iterator();
		while(termIterator.hasNext()){
			Integer term = termIterator.next();

			// cutoff is init size for the trimmed

			ArrayList<CoWeight> a = null;
			ArrayList<CoWeight> b = null;

			// try get coweight set for term from both days

			try{
				if(cosetArray.get(0).get(term).size() >= cutoff){
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
				if(cosetArray.get(1).get(term).size() >= cutoff){
					b = new ArrayList<CoWeight>(cosetArray.get(1).get(term).subList(0, cutoff - 1));
				}
				else{
					b = new ArrayList<CoWeight>(cosetArray.get(1).get(term));
				}
			}
			catch (NullPointerException e) {
				//  gulp!
			}

			// set default jaccard to 0.0

			Double jac = 0.0;

			// if either a or b is null, then jaccard is just z. 0 is kept in the loop to provide continuity

			if(a != null && b != null){	// if either are null jac == 0.0 will be added

				// convert arraylist to hashmap to make coweight access easy

				HashSet<Integer> aMap = new HashSet<Integer>(a.size());
				Iterator<CoWeight> aIter = a.iterator();

				while(aIter.hasNext()){
					CoWeight acw = aIter.next();
					aMap.add(acw.termId);
				}

				HashSet<Integer> bMap = new HashSet<Integer>(b.size());
				Iterator<CoWeight> bIter = b.iterator();
				while(bIter.hasNext()){
					CoWeight bcw = bIter.next();
					bMap.add(bcw.termId);
				}

				// get intersection

				HashSet<Integer> intersection = new HashSet<Integer>(aMap.size());				
				Iterator<Integer> aMapIter = aMap.iterator();

				while(aMapIter.hasNext()){
					Integer aTerm = aMapIter.next();

					if(bMap.contains(aTerm)){
						intersection.add(aTerm);
					}
				}

				// get union
				aMap.addAll(bMap);

				// get jaccard, dont bother if union is 0

				if(a.size() != 0){
					jac = (double)Math.round(((double)intersection.size() / (double)aMap.size()) * 1000) / 1000;
				}
			}

			// add jaccard value for the interval, if first time add term first

			if(!jaccardListNonWeighted.containsKey(term)){

				// init map of jaccard to interval and add j

				HashMap<Integer, Double> jEachDayMap = new HashMap<Integer, Double>(32);	// 17 + 16 = 33 days => 32 intervals

				// scratch that - pre init this map with 9.9 so we have an entry for every interval and we can distinguish the 9.9 as interval in which term was missing on a particular day!

				// pre init this map with 0.0

				for(int i =0 ; i < 32 ;i++){
					jEachDayMap.put(i, 0.0);
				}
				jEachDayMap.put(dayCounterNonWeighted, jac);

				// add mapping for term
				jaccardListNonWeighted.put(term, jEachDayMap);
			}
			else{
				jaccardListNonWeighted.get(term).put(dayCounterNonWeighted, jac);
			}

		}
		LOG.info("Jacard size = " + jaccardListNonWeighted.size());
		// LOG.info("Term error occured " + termError + "times." );
		dayCounterNonWeighted++; // must be incremented here to ensure continuity
	}

	/*** 
	 * 
	 * @param feed array of cosets for "today" and "yesterday"
	 * @return maintains a static hashMap of jaccards for each term over duration of corpus, access when finished traversing the corpus
	 * @throws IOException 
	 */
	public void getJaccardWeightedSimilarity(ArrayList<HashMap<Integer, ArrayList<CoWeight>>> cosetArray, int cutoff) throws IOException{
		LOG.info("Interval: " + (dayCounterWeighted+1));

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

			// HOW MANY TERMS IN COSET?

			ArrayList<CoWeight> a = null;
			ArrayList<CoWeight> b = null;


			try{
				if(cosetArray.get(0).get(term).size() >= cutoff){
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
				if(cosetArray.get(1).get(term).size() >= cutoff){
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
				jac = getEnhancedJaccardVal(a,b);
			}

			// add jaccard value for the interval, if first time add term first

			if(!jaccardListWeighted.containsKey(term)){

				// init map of jaccard to interval and add j
				HashMap<Integer, Double> jEachDayMap = new HashMap<Integer, Double>(32);	// 17 + 16 = 33 days => 32 intervals
				// pre init this map with 0.0
				for(int i =0 ; i < 32 ;i++){
					jEachDayMap.put(i, 0.0);
				}

				jEachDayMap.put(dayCounterWeighted, jac);

				// add mapping for term
				jaccardListWeighted.put(term, jEachDayMap);
			}
			else{
				jaccardListWeighted.get(term).put(dayCounterWeighted, jac);
			}

		}
		LOG.info("Jacard size = " + jaccardListWeighted.size());
		//		LOG.info("Term error occured " + termError + "times." );
		dayCounterWeighted++; // must be incremented here to ensure continuity
	}
	private static Double getEnhancedJaccardVal(ArrayList<CoWeight> a, ArrayList<CoWeight> b){

		// convert arraylist to hashmap to make coweight access easy

		HashMap<Integer, Double> aMap = new HashMap<Integer, Double>(a.size());
		Iterator<CoWeight> aIter = a.iterator();
		while(aIter.hasNext()){
			CoWeight acw = aIter.next();
			aMap.put(acw.termId, acw.correlate);
		}
		HashMap<Integer, Double> bMap = new HashMap<Integer, Double>(b.size());
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

			if(bMap.containsKey(aTerm)){
				// square the value
				jacc += Math.pow( Math.abs(bMap.get(aTerm) - aWeight), 2);
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

		// sqrt the sum of squares
		jacc = Math.sqrt(jacc);

		jacc = (double)Math.round((jacc ) * 1000) / 1000;

		// et voila!

		return jacc;
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

	public static void serializeJaccards(String outputPath, int index, double m) throws IOException{
		LOG.info("Serializing Weighted Jaccards.");
		String path = outputPath + "/" + String.valueOf(index) + "_" + String.valueOf(m) + "_jaccardWeighted.ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(jaccardListWeighted);
		objectOut.close();
		LOG.info("Finished serializing Weighted Jaccards.");
		
		LOG.info("Serializing Non-Weighted Jaccards.");
		String path2 = outputPath + "/" + String.valueOf(index) + "_" + String.valueOf(m) + "_jaccardNon_Weighted.ser";
		FileOutputStream fileOut2 = new FileOutputStream(path2);
		ObjectOutputStream objectOut2 = new ObjectOutputStream(fileOut2);
		objectOut2.flush();
		objectOut2.writeObject(jaccardListNonWeighted);
		objectOut2.close();
		LOG.info("Finished serializing Weighted Jaccards.");
	}
	public static void serializeJaccards(String outputPath) throws IOException{
		LOG.info("Serializing Weighted Jaccards.");
		String path = outputPath + "/jaccardWeighted.ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(jaccardListWeighted);
		objectOut.close();
		LOG.info("Finished serializing Weighted Jaccards.");
		
		LOG.info("Serializing Non-Weighted Jaccards.");
		String path2 = outputPath + "/jaccardNon_Weighted.ser";
		FileOutputStream fileOut2 = new FileOutputStream(path2);
		ObjectOutputStream objectOut2 = new ObjectOutputStream(fileOut2);
		objectOut2.flush();
		objectOut2.writeObject(jaccardListNonWeighted);
		objectOut2.close();
		LOG.info("Finished serializing Weighted Jaccards.");
	}
	public static void printResults() throws IOException{
		BufferedWriter out = new BufferedWriter(new FileWriter(TweetAnalysis.output));
		Iterator<Entry<Integer, HashMap<Integer, Double>>> termIter = jaccardListNonWeighted.entrySet().iterator();
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