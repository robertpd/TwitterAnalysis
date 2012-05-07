package com.twitter.corpus.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;
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
		File indexFile = new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/index100k.ser");
		File docTermsMapFile = new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/docTermsMap100k.ser");

		if(docTermsMapFile.exists() && indexFile.exists()){
			try{
				if(docTermsMapFile.exists()){
					ObjectInputStream ois = new ObjectInputStream(new FileInputStream(docTermsMapFile));
					docTermsMap = (HashMap<Long, ArrayList<Integer>>) ois.readObject();
					ois.close();
				}
				if(indexFile.exists()){
					ObjectInputStream ois = new ObjectInputStream(new FileInputStream(indexFile));
					termMatrix = (HashMap<Integer, HashSet<Long>>) ois.readObject();
					ois.close();
				}
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
					String tweet = status.getText();
					if (tweet == null){
						continue;
					}
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
					if(docNum > 100000){
						break;
					}
				}
			}
			finally
			{
			}
			try {
				if(termMatrix != null){
					FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/index100k.ser");
					ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
					objectOut.flush();
					objectOut.writeObject(termMatrix);
					objectOut.close();
				}
				if(docTermsMap != null){
					FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/docTermsMap100k.ser");
					ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
					objectOut.flush();
					objectOut.writeObject(docTermsMap);
					objectOut.close();
				}
			}
			catch(Exception e){
				//				if(inFile.exists()){
				//					inFile.delete();
				//				}
				e.printStackTrace();
			}

		}
		Runtime.getRuntime().gc();
//		HashMap<Integer, LinkedList<CoWeight>> coSetMap = new HashMap<Integer, LinkedList<CoWeight>>();
		HashMap<Integer, ArrayList<CoWeight>> coSetMapArray = new HashMap<Integer, ArrayList<CoWeight>>();

		// 	for term i
		CoWeight cs = new CoWeight(0, 0.0); 	// declare blank CoWeight, this object is reused with .clear() rather than create a new each time. Avoids a GC error
		Set<Integer> termMatrixKeys = termMatrix.keySet();
		for(Integer i : termMatrixKeys){
			long startTime = System.currentTimeMillis();
//			HashSet<Integer> termsDone = new HashSet<Integer>();	// removed to correct scope
			LinkedList<CoWeight> termCoSet = new LinkedList<CoWeight>();
			ArrayList<CoWeight> termCoSetArray = new ArrayList<CoWeight>(); 
			// get termWrap contains docCount, DocList
			HashSet<Long> docList = termMatrix.get(i);
			Integer termINum = termMatrix.get(i).size();
			int termIJNum;
			// for each doc..
			for(Long doc : docList){
				//				termIJNum = 0;	// this is the wrong place to zero termIJNum , i think
				int termJNum = 0;
				// for each term i in doc..	
				ArrayList<Integer> termList = docTermsMap.get(doc);
				HashSet<Integer> termsDone = new HashSet<Integer>();
				for(Integer term : termList){
					if(term == i){
						break;	//skip when i == term, ie. they are the same
					}
					if(!termsDone.contains(term)){	//don't want to do same term twice..
//						cs.clear();
						termIJNum = 0;
//						if(i == 171){	// 171 == rt
//							continue;
//						}
						HashSet<Long> termDocList = termMatrix.get(term);	// really necessary??? look up at doc : docList, here is the docId list...!
						termJNum = termDocList.size();
						// if term produces a smaller doc set, switch to that smaller set
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
							if(m > 0.0){
//								cs.clear();
								cs.termId = term;
								cs.correlate = m;
							}
//							termCoSet.add(cs);
							termCoSetArray.add(cs);
//							termCoSetArray.removeAll(Collections.singleton(null));
						}
					}
					termsDone.add(term);	// set a skip list for processed terms
				}
			}
//			coSetMap.put(i, termCoSet);
			coSetMapArray.put(i, termCoSetArray);
			Long currTime = System.currentTimeMillis();
			LOG.info("Term " + termBimap.inverse().get(i)+ " took: " + Admin.getTime(startTime, currTime));
		}
		FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/coset100.ser");
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(coSetMapArray);
		objectOut.close();
	}
}
