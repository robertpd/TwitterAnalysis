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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.log4j.Logger;
import org.omg.IOP.ENCODING_CDR_ENCAPS;

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
		// Get the union of the keys from both arrays
		ArrayList<HashMap<Integer, HashMap<Integer, Double>>> copyMap = new ArrayList<HashMap<Integer,HashMap<Integer, Double>>>(cosetArray);
		// TODO coset array contained somewhere 4370, think all entries are null and so size is zero... . why??

		Set<Integer> allTerms = new HashSet<Integer>(copyMap.get(0).keySet());
		//		Set<Integer> interSet = null;
		//		Set<Integer> unionSet = null;
		allTerms.addAll(copyMap.get(1).keySet());
		copyMap = null;

		// now have all unique keys and can iterate
		Iterator<Integer> termIterator = allTerms.iterator();
		while(termIterator.hasNext()){
			Integer term = termIterator.next();

			// need to re-arch to take only top 5 coweights
			//			try{
			//				interSet = new HashSet<Integer>(cosetArray.get(0).get(term).size());
			// copy incoming cosets
			//				HashMap<Integer, Double> termCosetA = cosetArray.get(0).get(term);	// grab term correlates from both hashmaps
			//				HashMap<Integer, Double> termCosetB = cosetArray.get(1).get(term);
			// init arraylist for ranked coweight entries

			int cutoff = 5;
			HashSet<Integer> a = null;
			HashSet<Integer> b = null;
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
				//  gulp!
			}

			Double jac = 0.0;
			if(a != null && b != null){	// if either are null jac == 0.0 will be added
				Set<Integer> inters = new HashSet<Integer>(cutoff);
				Set<Integer> unions =  new HashSet<Integer>(cutoff * 2);

				Iterator<Integer> aIter2 = a.iterator();
				while(aIter2.hasNext()){
					Integer a2 = aIter2.next();
					if(b.contains(a2)){
						inters.add(a2);
					}
				}
				a.addAll(b);
				unions.addAll(a);
				unions.addAll(b);			

				if(unions.size() != 0){
					jac = (double)Math.round(((double)inters.size() / (double)a.size()) * 1000) / 1000;
				}
			}
			if(!jaccardList.containsKey(term)){
				HashMap<Integer, Double> jEachDayMap = new HashMap<Integer, Double>(33);	// 33 intervals
				jEachDayMap.put(dayCounter, jac);
				jaccardList.put(term, jEachDayMap);
			}
			else{
				jaccardList.get(term).put(dayCounter, jac);
			}

			//				int unionInit = 0;
			//				if( termCosetA != null ) { unionInit += termCosetA.size(); }
			//				if(termCosetB!=null){ unionInit += termCosetB.size(); }
			//
			//				unionSet = new HashSet<Integer>(unionInit);
			//				if(termCosetA != null){ unionSet.addAll(termCosetA.keySet()); }
			//				if(termCosetB != null){	unionSet.addAll(termCosetB.keySet()); }

			// if term doesn't occur on particular day (unlightly, given thresholds performed already), termCoset will be null, this would give a 0 jaccard
			//				if(termCosetB != null && termCosetA != null){
			//					// iterate A, get coweight and check i
			//					Iterator<Integer> aIter = termCosetA.keySet().iterator();
			//					while(aIter.hasNext()){
			//						Integer t = aIter.next();
			//						if(termCosetB.containsKey(t)){
			//							interSet.add(t);
			//						}
			//					}
			//					termCosetB.keySet().removeAll(interSet);	// remove all keys that have already been detected
			//					Iterator<Integer> iterB = termCosetB.keySet().iterator();
			//					while(iterB.hasNext()){
			//						Integer t = iterB.next();
			//						if(termCosetA.containsKey(t)){
			//							interSet.add(t);
			//						}
			//					}
			//					double jaccard = 0.0;
			//					if(unionSet.size() == 0){ unionZero++;}
			//					if(interSet.size() > 0 && unionSet.size() > 0){
			//						jaccard = (double)interSet.size() / (double) unionSet.size();	// switch to union
			//					}
			//					else{
			//						jaccard = 0.0; 	// set jaccard = 0 as a placeholder, for diffing want to check agains zero
			//					}

			//					if(!jaccardList.containsKey(term)){
			//						HashMap<Integer, Double> jEachDayMap = new HashMap<Integer, Double>(33);	// 33 intervals
			//						jEachDayMap.put(dayCounter, jaccard);
			//						jaccardList.put(term, jEachDayMap);
			//					}
			//					else{
			//						jaccardList.get(term).put(dayCounter, jaccard);
			//					}
			//				}
			//				else{
			//					// else one of set a or b is null...
			//					termError++;
			//					LOG.info("termCosetA or termCosetB is null.");
			//				}
			//			}
			//			catch(NullPointerException np){
			//				termError++;
			//				LOG.info("NullPointerException at Line 102 Jaccard, a term is not present one of the days");
			//			}
		}
		LOG.info("Jacard size = " + jaccardList.size());
		LOG.info("Term error occured " + termError + "times." );
		dayCounter++; // must be incremented here to ensure continuity
	}
	/***
	 * Calculate jaccard differences across intervals.
	 *  @return HashMap of term and jaccard differences for each interval
	 */
	public static HashMap<Integer, ArrayList<Double>> calcJaccardDifferences(){
		LOG.info("Calc'ing jaccard differences");
		HashMap<Integer , ArrayList<Double>> retVal = new HashMap<Integer, ArrayList<Double>>(jaccardList.size());
		Iterator<Entry<Integer, HashMap<Integer, Double>>> jaccardIter = jaccardList.entrySet().iterator();

		while(jaccardIter.hasNext()){
			Entry<Integer, HashMap<Integer, Double>> termJSet = jaccardIter.next();
			HashMap<Integer, Double> termJIntervals = termJSet.getValue();
			Iterator<Entry<Integer, Double>> jaccardSetIter = termJIntervals.entrySet().iterator();
			Double last = 0.0;
			boolean firstIter = false;

			while(jaccardSetIter.hasNext()){

				Entry<Integer, Double> jVal = jaccardSetIter.next();
				Double value = jVal.getValue();
				if(!firstIter){	// exec once to buffer 1st jaccard value
					firstIter = true;
					last = value;
					continue;
				}

				if(!retVal.containsKey(termJSet.getKey())){
					ArrayList<Double> intervalDiffs = new ArrayList<Double>(33);
					// can we be sure that the intervalDiffs array will be filled from 0?? YES
					intervalDiffs.add(jVal.getKey(), value - last);
					retVal.put(termJSet.getKey(), intervalDiffs);
				}
				else{
					retVal.get(termJSet.getKey()).add(jVal.getKey(), value-last);
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
		// iterator for termCosetA
		Iterator<Entry<Integer, Double>> coweightAIter = termCoset.entrySet().iterator();
		while(coweightAIter.hasNext()){
			top5Aranked.add(coweightAIter.next());
		}
		// sort by weight
		Collections.sort(top5Aranked, new top5Comp());
		// init sorted top 5 list
		ArrayList<Integer> sortedTop5A = new ArrayList<Integer>(cutoff);
		Iterator<Entry<Integer, Double>> liter = top5Aranked.iterator();
		int breakCounter = 0;
		while(liter.hasNext() && breakCounter < cutoff){
			// i can imagine that future enhancements will require the coweight type to keep term and weight together
			sortedTop5A.add(liter.next().getKey());
			breakCounter++;
		}
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
	public static void printResults() throws IOException{
		BufferedWriter out = new BufferedWriter(new FileWriter(TweetAnalysis.jaccardOutput));
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