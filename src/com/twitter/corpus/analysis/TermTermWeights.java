package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;
import com.twitter.corpus.demo.Admin;
import com.twitter.corpus.demo.ProcessedTweet;
import com.twitter.corpus.demo.TweetProcessor;
import com.twitter.corpus.types.CoWeight;

public class TermTermWeights implements java.io.Serializable{
	private static final long serialVersionUID = -3094140138580705422L;
	private static final Logger LOG = Logger.getLogger(TermTermWeights.class);
	public static int counter =1;
	public static HashSet<String> users = new HashSet<String>(10000);
	public static HashBiMap<String,Integer> termBimap = HashBiMap.create();
	// docTermsMap -> docId, list of termIds
	public static HashMap<Long, ArrayList<Integer>> docTermsMap = new HashMap<Long, ArrayList<Integer>>(); 

	public TermTermWeights(HashMap<Integer, HashSet<Long>> termIndex) throws IOException{
		this.termIndex = termIndex;
	}
	
	private HashMap<Integer, HashSet<Long>> termIndex = null;
	/**
	 * 
	 * 		@return Returns a HashMap of term and weighted correlates.
 	* 		@throws IOException
 	*/
	public HashMap<Integer, ArrayList<CoWeight>> termCosetBuilder() throws IOException{

		HashMap<Integer, ArrayList<CoWeight>> coSetMapArray = new HashMap<Integer, ArrayList<CoWeight>>();

		// 	for term i
		//		CoWeight cs = new CoWeight(0, 0.0); 	// declare blank CoWeight, this object is reused with .clear() rather than create a new each time. Avoids a GC error ?? really?
		int cnt=0;
		long lastTime2=System.currentTimeMillis();
		Set<Integer> termMatrixKeys = termIndex.keySet();
		for(Integer i : termMatrixKeys){
			ArrayList<CoWeight> termCoSetArray = new ArrayList<CoWeight>(); 
			HashSet<Long> docList = termIndex.get(i);
			Integer termINum = termIndex.get(i).size();
			if(termINum > 1){
				int termIJNum=0;
				HashSet<Integer> uniqueTerms = null;

				int uniqueTermsSize=0;
				for(Long doc : docList){
					uniqueTermsSize += docTermsMap.get(doc).size();
				}
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
				for(Iterator<Integer> term = uniqueTerms.iterator(); term.hasNext();){
					int termJ = term.next();
					HashSet<Long> termDocList = termIndex.get(termJ);
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
					if(denom > 0){
						double m = (double)termIJNum / (double)denom;
						termIJNum = 0;
						CoWeight cs = null;
						m = (double)Math.round(m * 1000) / 1000;
						if(m > 0.05){
							cs = new CoWeight(termJ, m);
						}
						termCoSetArray.add(cs);
						cs = null;
					}
					uniqueTerms.remove(term);
				}

				//sort le coset array aroundabout here!
				termCoSetArray.removeAll(Collections.singleton(null));
				Collections.sort(termCoSetArray,  new CoWeightComparator());
				coSetMapArray.put(i, termCoSetArray);
				cnt++;
				if(cnt % 1000 ==0){
					Long currTime2 = System.currentTimeMillis();
					LOG.info(cnt + " terms didnt exactly take " + Admin.getTime(lastTime2, currTime2));
					lastTime2 = currTime2;
				}
			}// doc count filter
		}

		return coSetMapArray;
		// slight modifaction to program flow aboveth lightly:-)

		//		List<Entry<Integer, ArrayList<CoWeight>>> CoSetArrayList = new ArrayList<Entry<Integer, ArrayList<CoWeight>>>(coSetMapArray.entrySet());
		//
		//		Collections.sort(CoSetArrayList, new CoSetComparator());
		//
		//		try{
		//			BufferedWriter out = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/coset_400k__0_05m__sorted_3.txt"));
		//			if((CoSetArrayList != null )&& (termBimap != null)){
		//				for(int i=0; i < CoSetArrayList.size(); i++){
		//					ArrayList<CoWeight> cwArl = CoSetArrayList.get(i).getValue();
		//					int term = CoSetArrayList.get(i).getKey();
		//					StringBuffer sb = new StringBuffer();
		//					int docCount = termMatrix.get(term).size();
		////					if(docCount > 10){
		//					sb.append(termBimap.inverse().get(term) + " [" + docCount + "]" + " { ");
		//					boolean createLine=false;
		//					for(CoWeight cw: cwArl){
		//						if(cw != null){
		////								if(cw.correlate > 0.2){
		//							createLine = true;
		//							sb.append(termBimap.inverse().get(cw.termId)+ ": " + cw.correlate + ", ");						
		////								}
		//						}
		//					}
		//					if(createLine){
		//						out.write(sb.replace(sb.length()-1, sb.length()-1,"").append(" }\n").toString());
		//					}
		//				}
		//				out.close();
		//			}
		//			System.out.print("finito");
		//		}catch(Exception ex){
		//			System.out.print("asd");
		//		}
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
