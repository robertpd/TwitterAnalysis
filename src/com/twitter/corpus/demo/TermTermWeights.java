package com.twitter.corpus.demo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;
import com.twitter.corpus.demo.TermTermWeights.CustomComparator;
import com.twitter.corpus.types.CoWeight;

public class TermTermWeights implements java.io.Serializable{
	private static final long serialVersionUID = -3094140138580705422L;

	public TermTermWeights(StatusStream stream) throws IOException{
		this.stream = stream;
	}
	private static final Logger LOG = Logger.getLogger(TermTermWeights.class);
	public static HashBiMap<String,Integer> termBimap = HashBiMap.create();
	// long docid, 
	public static HashMap<Long, ArrayList<Integer>> docTermsMap = new HashMap<Long, ArrayList<Integer>>(); 
	private StatusStream stream;
	private HashMap<Integer, HashSet<Long>> termMatrix;// = new HashMap<Integer, TermWrap>();

	@SuppressWarnings("unchecked")
	public void Index() throws IOException{
		File indexFile = new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/index5.ser");
		File docTermsMapFile = new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/docTermsMap5.ser");
		File termBiMapFile = new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/termBiMap5.ser");
		int rt=0;
		if(docTermsMapFile.exists() && indexFile.exists() && termBiMapFile.exists()){
			try{			
				ObjectInputStream docTermMapois = new ObjectInputStream(new FileInputStream(docTermsMapFile));
				docTermsMap = (HashMap<Long, ArrayList<Integer>>) docTermMapois.readObject();
				docTermMapois.close();				

				ObjectInputStream indexois = new ObjectInputStream(new FileInputStream(indexFile));
				termMatrix = (HashMap<Integer, HashSet<Long>>) indexois.readObject();
				indexois.close();

				ObjectInputStream termBiMapois = new ObjectInputStream(new FileInputStream(termBiMapFile));
				termBimap = (HashBiMap<String,Integer>) termBiMapois.readObject();
				termBiMapois.close();				
			}
			catch(Exception e){}
		}
		else{
			TweetProcessor.callStops();
			termMatrix = new HashMap<Integer, HashSet<Long>>();
			int docNum=0;
			Status status;
			Long lastTime = 0l;
			try {
				while ((status = stream.next()) != null)
				{
					if(status.getHttpStatusCode() == 302){
						rt++;
						continue;
					}					
					String tweet = status.getText();
					if (tweet == null){	continue;}
					ProcessedTweet pt = TweetProcessor.processTweet(status.getText(),status.getId());

					for(int i=0; i< pt.termIdList.size() ; i++){
						if(!termMatrix.containsKey(pt.termIdList.get(i)))
						{// tdh - termDocHash
							HashSet<Long> tdh = new HashSet<Long>();
							if(!tdh.contains(status.getId())){
								tdh.add(status.getId());
							}
							termMatrix.put(pt.termIdList.get(i), tdh);
						}
						else
						{
							if(!termMatrix.get(pt.termIdList.get(i)).contains(status.getId())){
								termMatrix.get(pt.termIdList.get(i)).add(status.getId());
							}
						}
					}
					docNum++;
					if(docNum % 10000 == 0 ){
						Long currTime = System.currentTimeMillis();
						LOG.info(docNum + " tweets processed in " +  Admin.getTime(lastTime, currTime));
						lastTime = currTime;

					}
					if(docNum > 40000){
						LOG.info(termMatrix.size() + " total terms.");
						break;
					}
				}
				try {
					if(termBimap != null){
						//						FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/termBiMap100k.ser");
						//						ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
						//						objectOut.flush();
						//						objectOut.writeObject(termBimap);
						//						objectOut.close();
					}
				}
				catch(Exception e){	}
			}
			finally
			{
			}
			try {
				if(termMatrix != null){
					//					FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/index500k.ser");
					//					ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
					//					objectOut.flush();
					//					objectOut.writeObject(termMatrix);
					//					objectOut.close();
				}
				if(docTermsMap != null){
					//					FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/docTermsMap500k.ser");
					//					ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
					//					objectOut.flush();
					//					objectOut.writeObject(docTermsMap);
					//					objectOut.close();
				}
			}
			catch(Exception e){}



		}
		Runtime.getRuntime().gc();
		//		HashMap<Integer, LinkedList<CoWeight>> coSetMap = new HashMap<Integer, LinkedList<CoWeight>>();
		HashMap<Integer, ArrayList<CoWeight>> coSetMapArray = new HashMap<Integer, ArrayList<CoWeight>>();

		// 	for term i
		//		CoWeight cs = new CoWeight(0, 0.0); 	// declare blank CoWeight, this object is reused with .clear() rather than create a new each time. Avoids a GC error ?? really?
		int cnt=0;
		long lastTime2=System.currentTimeMillis();
		Set<Integer> termMatrixKeys = termMatrix.keySet();
		for(Integer i : termMatrixKeys){
			ArrayList<CoWeight> termCoSetArray = new ArrayList<CoWeight>(); 
			HashSet<Long> docList = termMatrix.get(i);
			Integer termINum = termMatrix.get(i).size();
			if(termINum > 5){
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
					HashSet<Long> termDocList = termMatrix.get(termJ);
					int termJNum = termDocList.size();
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
						if(m > 0.1){
							cs = new CoWeight(termJ, m);
						}
						termCoSetArray.add(cs);
						cs = null;
					}
					uniqueTerms.remove(term);
				}

				//sort le coset array aroundabout here!
				termCoSetArray.removeAll(Collections.singleton(null));
				Collections.sort(termCoSetArray,  new CustomComparator());
				coSetMapArray.put(i, termCoSetArray);
				cnt++;
				if(cnt % 1000 ==0){
					Long currTime2 = System.currentTimeMillis();
					LOG.info(cnt + " terms didnt exactly take " + Admin.getTime(lastTime2, currTime2));
					lastTime2 = currTime2;
				}
			}
		}

		//		FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/coset11k.ser");
		//		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		//		objectOut.flush();
		//		objectOut.writeObject(coSetMapArray);
		//		objectOut.close();

		//#######################################################################################################################

		//		File cosetFile = new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/coset11k.ser");
		//		try{
		//			ObjectInputStream docTermMapois = new ObjectInputStream(new FileInputStream(cosetFile));
		//			coSetMapArray = (HashMap<Integer, ArrayList<CoWeight>>) docTermMapois.readObject();
		//			docTermMapois.close();
		//		}
		//		catch(Exception ex){}

		//		File termBiMapFile = new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/termBiMap11k.ser");
		//		HashBiMap<String,Integer> termBimap;// = HashBiMap.create();
		try{
			//			ObjectInputStream docTermMapois = new ObjectInputStream(new FileInputStream(termBiMapFile));
			//			termBimap = (HashBiMap<String,Integer>) docTermMapois.readObject();
			//			docTermMapois.close();

			BufferedWriter out = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/cosetX.txt"));
			Set<Integer> keyset = coSetMapArray.keySet();
			if((coSetMapArray != null )&& (termBimap != null)){
				for(Integer c : keyset){
					ArrayList<CoWeight> cwList = coSetMapArray.get(c);
					StringBuffer sb = new StringBuffer();
					int docCount = termMatrix.get(c).size();
					//					if(docCount > 10){
					sb.append(termBimap.inverse().get(c) + " [" + docCount + "]" + " { ");
					boolean createLine=false;
					for(CoWeight cw: cwList){
						if(cw != null){
							//								if(cw.correlate > 0.2){
							createLine = true;
							sb.append(termBimap.inverse().get(cw.termId)+ ": " + cw.correlate + ", ");						
							//								}
						}
					}
					if(createLine){
						out.write(sb.append("}\n").toString());
					}
					//					}
				}
				out.close();
			}
			System.out.print("finito");
		}catch(Exception ex){
			System.out.print("asd");
		}
	}
	public class CustomComparator implements Comparator<CoWeight> {
		@Override
		public int compare(CoWeight c1, CoWeight c2) {
			return c1.compareTo(c2);//c1.getCorrelate().compareTo(c2.getCorrelate());
		}
	}
}
