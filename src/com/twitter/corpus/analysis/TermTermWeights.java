package com.twitter.corpus.analysis;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.twitter.corpus.demo.Admin;
import com.twitter.corpus.demo.TweetAnalysis;
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
	 * <p>TermCosetBuilder:
	 * 			return a map of coweights for terms.<br>
	 * 			Ignores terms that < 15 times.<br>
	 * 			Ignores coweights < 0.05.<br>
	 * </p>
	 * 
	 * 		@return Returns a HashMap of term and weighted correlates.
	 * 		@throws IOException
	 */
	public HashMap<Integer, ArrayList<CoWeight>> termCosetBuilder(double correlateCutoff) throws IOException{
		LOG.info("Starting term coweighting...");		
		int cnt=0;	// counter for LOG.info()
		long lastTime2=System.currentTimeMillis();	// timing for LOG.info()

		// return object cosetMap, contains terms mapped to coweights
		HashMap<Integer, ArrayList<CoWeight>> cosetMap = new HashMap<Integer, ArrayList<CoWeight>>(termIndex.size());

		// TODO BIGGEST 2ND: termIndex was null the second time around, how???
		//		HashMap<Integer, ArrayList<CoWeight>> coset2 = Maps.newHashMapWithExpectedSize(termIndex.size());
		//		for(Integer i : termIndex.keySet()){
		//			coset2.put(i, null);
		//		}

		Iterator<Map.Entry<Integer, HashSet<Long>>> termMatrixIter = termIndex.entrySet().iterator();
		//		for(Integer i : termMatrixKeys){
		while(termMatrixIter.hasNext()){
			Integer i = termMatrixIter.next().getKey();
			
			// the following will cause a significant blunder in the algorithm
			// it will omit terms of certain tf as KEYS in the coset map 
			// however
			// these terms will still exist in local index and will influence coset weightings. see uniqueTerms below
//			if(TweetAnalysis.corpusIndex.get(i).size() < low || TweetAnalysis.corpusIndex.get(i).size() > hi){
//				continue;
//			}
			
			
			// local analysis; get docset from intervalIndex 
			HashSet<Long> docList = termIndex.get(i);							// doclist -> list of docs for a term
			// global weighting of terms comes from Global corpus index!!
			Integer termINum = termIndex.get(i).size();		// number of documents with term "i"

			

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

			// coweight array for term "i"
			ArrayList<CoWeight> termCoSetArray = new ArrayList<CoWeight>(uniqueTerms.size());		// new coset array should have same dim as termMatrix...

			Iterator<Integer> uniqueTermsIter = uniqueTerms.iterator();
			while(uniqueTermsIter.hasNext()){
				int termJ = uniqueTermsIter.next();
				HashSet<Long> termDocList = termIndex.get(termJ);
				// can get null here, we iterate uniqueTerms and get doc lists from termIndex. low/hi freq terms are removed from termIndex but may persist in uniqueTerms 
				//TODO sort this shit out soldier
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
				double m = (double)termIJNum / (double)(termINum + termJNum - termIJNum);
				m = (double)Math.round(m * 1000) / 1000;

				// ignore low coweights
				if(m > correlateCutoff){
					CoWeight cs = new CoWeight(termJ, m);
					termCoSetArray.add(cs);
				}
			}
			Collections.sort(termCoSetArray, new CoWeightComparator());
			cosetMap.put(i, termCoSetArray);

			cnt++;
			//			if(cnt % 1000 ==0){
			//				Long currTime2 = System.currentTimeMillis();
			//				LOG.info(cnt + " term coweights calculated in: " + Admin.getTime(lastTime2, currTime2));
			//				lastTime2 = currTime2;
			//				docCount1000=0;
			//			}
		}
		Long currTime2 = System.currentTimeMillis();
		LOG.info(cnt + " term coweights calculated in: " + Admin.getTime(lastTime2, currTime2));
		lastTime2 = currTime2;
		return cosetMap;
	}
	public static void serializeTermBimap(String path) throws IOException{
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(TermTermWeights.termBimap);
		objectOut.close();
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
