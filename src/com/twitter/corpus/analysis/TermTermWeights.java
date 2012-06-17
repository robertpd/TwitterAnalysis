package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.twitter.corpus.demo.Admin;
import com.twitter.corpus.types.CoWeight;

public class TermTermWeights implements java.io.Serializable{
	private static final long serialVersionUID = -3094140138580705422L;
	private static final Logger LOG = Logger.getLogger(TermTermWeights.class);
	public static int counter =1;
	public static HashBiMap<String,Integer> termBimap = HashBiMap.create();
	// docTermsMap -> docId, list of termIds
	public static HashMap<Long, ArrayList<Integer>> docTermsMap = new HashMap<Long, ArrayList<Integer>>();

	public static int docCount1000=0;

	public TermTermWeights(HashMap<Integer, HashSet<Long>> termIndex) throws IOException{
		this.termIndex = termIndex;
	}

	private HashMap<Integer, HashSet<Long>> termIndex = null;
	/**
	 * 
	 * 		@return Returns a HashMap of term and weighted correlates.
	 * 		@throws IOException
	 */
	public HashMap<Integer, HashMap<Integer, Double>> termCosetBuilder() throws IOException{

		//TODO BIGGEST TODO: remove terms with oob frequency
//		HashMap<Integer, ArrayList<CoWeight>> coSetMapArray = new HashMap<Integer, ArrayList<CoWeight>>();
		HashMap<Integer, HashMap<Integer, Double>> cosetMap = new HashMap<Integer, HashMap<Integer, Double>>();

		// 	for term i
		//		CoWeight cs = new CoWeight(0, 0.0); 	// declare blank CoWeight, this object is reused with .clear() rather than create a new each time. Avoids a GC error ?? really?
		int cnt=0;
		long lastTime2=System.currentTimeMillis();
//		Set<Integer> termMatrixKeys = termIndex.keySet();
		// TODO BIGGEST 2ND: termIndex was null the second time around, how???
		HashMap<Integer,ArrayList<CoWeight>> coset2 = Maps.newHashMapWithExpectedSize(termIndex.size());
		for(Integer i : termIndex.keySet()){
			coset2.put(i, null);
		}
		Iterator<Map.Entry<Integer, HashSet<Long>>> termMatrixIter = termIndex.entrySet().iterator();
		// iterate keys on termMatrix
		//		for(Integer i : termMatrixKeys){
		while(termMatrixIter.hasNext()){
			Integer i = termMatrixIter.next().getKey();
			HashSet<Long> docList = termIndex.get(i);							// doclist -> list of docs for a term
			Integer termINum = termIndex.get(i).size();		// number of documents with term "i"

			docCount1000+=termINum;
			int termIJNum=0;
			HashSet<Integer> uniqueTerms = null;

			// calc size of unique terms array..
			int uniqueTermsSize=0;
			for(Long doc : docList){
				uniqueTermsSize += docTermsMap.get(doc).size();
			}

			// get the unique terms to process of documents of term i, remove term i itself..
			uniqueTerms = new HashSet<Integer>(uniqueTermsSize);
			for(Long doc : docList){
				ArrayList<Integer> termList = docTermsMap.get(doc);
				for(Integer term: termList){
					if(!uniqueTerms.contains(term)){
						uniqueTerms.add(term);
					}
				}
				uniqueTerms.remove(i);
				termList = null;
			}

			// confusing, change this
			//				Iterator<Integer> uniqueTermIter = uniqueTerms.iterator();
			//				while(uniqueTermIter.hasNext()){
			//					int termJ = uniqueTermIter.next();
			//				}

			// coweight array for term "i"
//			ArrayList<CoWeight> termCoSetArray = new ArrayList<CoWeight>(uniqueTerms.size());		// new coset array should have same dim as termMatrix...
//			Set<CoWeight> termCosetSet = new HashSet<CoWeight>(uniqueTerms.size());
			HashMap<Integer,Double> termCosetMap = new HashMap<Integer, Double>(uniqueTerms.size());
			
			for(Iterator<Integer> term = uniqueTerms.iterator(); term.hasNext();){
				int termJ = term.next();
				HashSet<Long> termDocList = termIndex.get(termJ);
				// can get null here, we iterate uniqueTerms and get doc lists from termIndex. low/hi freq terms are removed from termIndex but may persist in uniqueTerms
				if(termDocList == null){
					continue;
				}
				int termJNum = termDocList.size();
				termIJNum = 0;

				if(termINum < termJNum){
					for(Long document :docList){
						if(termDocList.contains(document)){
							termIJNum++;
						}
					}
				}
				else{
					for(Long document :termDocList){
						if(docList.contains(document)){
							termIJNum++;
						}
					}
				}

				// termINum => num docs with I, termJNum => w/ J, IJ => docs with both
				int denom = termINum + termJNum - termIJNum;
				// ilogical to have a div0
				//					if(denom > 0){
				double m = (double)termIJNum / (double)denom;
				termIJNum = 0;
				CoWeight cs = null;
				m = (double)Math.round(m * 1000) / 1000;
				if(m > 0.05){
//					cs = new CoWeight(termJ, m);// old, replaced by below
					termCosetMap.put(termJ, m);
				}
				// arraylist of coweights, to be added to hashmap of terms to coweights
//				termCosetSet.add(cs);// the set replacement. This has now been replaced also; by a hashMap
//				termCoSetArray.add(cs); // this was the first imp, used an ArrayList
//				cs = null;
				//					}
			}

			//TODO Coset collection has been converted from ArrayList to HashSet, verify equals and hashcode methods for correctness
//			termCoSetArray.removeAll(Collections.singleton(null));
//			Collections.sort(termCoSetArray,  new CoWeightComparator());
//			coSetMapArray.put(i, termCoSetArray);
			
			//TODO NEED TO SORT THIS as treeset has been reverted
//			termCosetSet.removeAll(Collections.singleton(null));// meh, this wasnt needed, nulls result from resizing according to bucket ratio
//			HashSet<CoWeight> treeSet = new HashSet<CoWeight>();
//			treeSet.addAll(termCosetSet);
			
			cosetMap.put(i, termCosetMap);
			
			cnt++;
			if(cnt % 1000 ==0){
				Long currTime2 = System.currentTimeMillis();
				LOG.info(cnt + " term coweights calculated in: " + Admin.getTime(lastTime2, currTime2));
				lastTime2 = currTime2;
				docCount1000=0;
			}
		}
		return cosetMap;
	}
	public class CoWeightComparator implements Comparator<CoWeight> {
		@Override
		public int compare(CoWeight c1, CoWeight c2) {
			return c1.compareTo(c2);
		}
	}
	public class CoSetComparator implements Comparator<Entry<Integer, ArrayList<CoWeight>>>{
		@Override
		public int compare(Entry<Integer, ArrayList<CoWeight>> o1, Entry<Integer, ArrayList<CoWeight>> o2) {
			if(termIndex.get(o1.getKey()).size() > termIndex.get(o2.getKey()).size())
				return -1;
			else if(termIndex.get(o1.getKey()).size() < termIndex.get(o2.getKey()).size())
				return 1;
			else
				return 0;
		}
	}
}
