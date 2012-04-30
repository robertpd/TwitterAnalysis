package com.twitter.corpus.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;

public class TermTermWeights implements java.io.Serializable{
	private static final long serialVersionUID = -3094140138580705422L;

	public TermTermWeights(StatusStream stream) throws IOException{
		this.stream = stream;
	}
	private static final Logger LOG = Logger.getLogger(TermTermWeights.class);
	public static HashBiMap<String,Integer> termBimap = HashBiMap.create();
	public static HashMap<Long, ArrayList<Integer>> docTermsMap = new HashMap<Long, ArrayList<Integer>>(); 
	private StatusStream stream;
	private HashMap<Integer, TermDocHash> termMatrix;// = new HashMap<Integer, TermWrap>();

	@SuppressWarnings("unchecked")
	public void Index() throws IOException{
		File inFile = new File("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/index.ser");

		if(inFile.exists()){
			try{
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(inFile));
				termMatrix = (HashMap<Integer, TermDocHash>) ois.readObject();
				ois.close(); 
			}
			catch(Exception e){}
		}
		else{
			TweetProcessor.callStops();
			termMatrix = new HashMap<Integer, TermDocHash>();
			int cnt = 0;
			int docNum=0;
			Status status;
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
						{
							TermDocHash tdh = new TermDocHash();
							if(!tdh.docHash.contains(status.getId())){
								tdh.docHash.add(status.getId());
							}
							termMatrix.put(pt.termIdList.get(i), tdh);
						}
						else
						{
							if(!termMatrix.get(pt.termIdList.get(i)).docHash.contains(status.getId())){
								termMatrix.get(pt.termIdList.get(i)).docHash.add(status.getId());	// append to linkedlist, will need to be converted to array??? for access speed,, not a sparse array, still flat just taking size of linkedlist as initializer
							}
						}
					}
					docNum++;
					if(docNum % 10000 == 0 ){
						LOG.info(docNum + "tweets processed");
					}
				}
			}
			finally
			{
			}
			try {
				if(termMatrix != null){
					FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/dirindex.ser");
					ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
					objectOut.flush();
					objectOut.writeObject(termMatrix);
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
		HashMap<Integer, LinkedList<CoWeight>> coSetMap = new HashMap<Integer, LinkedList<CoWeight>>();

		// 	for term i
//		Set<Integer> termMatrixKeys = termMatrix.keySet();
//		for(Integer i : termMatrixKeys){
//			long startTime = System.currentTimeMillis();
//			HashSet<Integer> termsDone = new HashSet<Integer>();
//			LinkedList<CoWeight> termCoSet = new LinkedList<CoWeight>();
//			// get termWrap contains docCount, DocList
//			HashSet<Long> docList = termMatrix.get(i).docHash;
//			Integer termINum = termMatrix.get(i).docHash.size();
//			int termIJNum;
//			// for each doc..
//			for(Long doc : docList){
//				//				termIJNum = 0;	// this is the wrong place to zero termIJNum , i think
//				int termJNum = 0;
//				// for each term i in doc..	
//				CoWeight cs = null;
//				ArrayList<Integer> termList = docTermsMap.get(doc);
//				for(Integer term : termList){
//					if(!termsDone.contains(term)){	//don't want to do same term twice..
//						termIJNum = 0;
//						if(i == 171){	// 171 == rt
//							continue;
//						}
//						HashSet<Long> termDocList = termMatrix.get(term).docHash;
//						termJNum = termDocList.size();
//						// if term produces a smaller doc set, switch to that smaller set
//						if(termINum < termJNum){
//							for(Long document :docList){
//								if(termDocList.contains(document)){
//									termIJNum++;
//								}
//							}
//						}
//						else{
//							for(Long document :termDocList){
//								if(docList.contains(document)){
//									termIJNum++;
//								}
//							}
//						}
//						int denom = termINum + termJNum - termIJNum;
//						if(denom > 0){
//							double m = (double)termIJNum / (double)denom;
//							if(m > 0.0){
//								cs = new CoWeight(term, m);
//							}
//							termCoSet.add(cs);
//						}
//					}
//					termsDone.add(term);	// set a skip list for processed terms
//				}
//			}
//			coSetMap.put(i, termCoSet);
//			Long currTime = System.currentTimeMillis();
//			LOG.info("Term " + termBimap.inverse().get(i)+ " took: " + Admin.getTime(startTime, currTime));
//		}
	}
//	private class TermWrap implements java.io.Serializable{
//		TermWrap(){
//			this.docHash = new HashSet<Long>();
//		}
//		public HashSet<Long> docHash;
//	}
	private class CoWeight{
		public CoWeight(int t, double c){
			this.termId = t;
			this.correlate = c;
		}
		int termId;
		double correlate;
	}
	void serIndex(HashMap<Integer, TermDocHash> termMatrix){
		try {
			if(termMatrix != null){
				FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/index.ser");
				ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
				objectOut.flush();
				objectOut.writeObject(termMatrix);
				objectOut.close();
			}
		}
		catch(Exception e){
//			if(inFile.exists()){
//				inFile.delete();
//			}
			e.printStackTrace();
		}
	}
}
