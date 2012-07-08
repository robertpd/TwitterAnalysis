package com.twitter.corpus.demo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
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

public class TermTermWeightsold_newOneHasTTWname implements java.io.Serializable{
	private static final long serialVersionUID = -3094140138580705422L;

	public TermTermWeightsold_newOneHasTTWname(StatusStream stream) throws IOException{
		this.stream = stream;
	}
	private static final Logger LOG = Logger.getLogger(TermTermWeightsold_newOneHasTTWname.class);
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
					if(docNum > 300000){
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
		long lastTime2=0;
		Set<Integer> termMatrixKeys = termMatrix.keySet();
		for(Integer i : termMatrixKeys){
			long startTime = System.currentTimeMillis();
			//			HashSet<Integer> termsDone = new HashSet<Integer>();	// removed to correct scope
			LinkedList<CoWeight> termCoSet = new LinkedList<CoWeight>();
			ArrayList<CoWeight> termCoSetArray = new ArrayList<CoWeight>(); 
			HashSet<Long> docList = termMatrix.get(i);
			Integer termINum = termMatrix.get(i).size();
			int termIJNum;
			
			int tdSize=0;
			for(Long doc : docList){
				tdSize += docTermsMap.get(doc).size();
			}
			HashSet<Integer> termsDone = null;
			termsDone = new HashSet<Integer>((tdSize/4));
			for(Long doc : docList){
				//				termIJNum = 0;	// this is the wrong place to zero termIJNum , i think
				int termJNum = 0;
				// for each term i in doc..	
				ArrayList<Integer> termList = docTermsMap.get(doc);
				for(Integer term : termList){
					if(term == i){
						continue;	//skip when i == term, ie. they are the same
					}
					termsDone.add(i);
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
							CoWeight cs = null;
							if(m > 0.0){
								//								cs = new CoWeight(term, (double)(((long)(m*100))/100));
								cs = new CoWeight(term, m);
							}
							termCoSetArray.add(cs);
						}
					}
					termsDone.add(term);	// set a skip list for processed terms
				}
			}
			//			coSetMap.put(i, termCoSet);
			coSetMapArray.put(i, termCoSetArray);
			Long currTime = System.currentTimeMillis();
			cnt++;
			if(cnt % 1000 ==0){
				//			LOG.info("Term " + termBimap.get(i) + " " +termBimap.inverse().get(i)+ " took: " + Admin.getTime(startTime, currTime));
				Long currTime2 = System.currentTimeMillis();
				LOG.info(cnt + " terms didnt exactly take " + Admin.getTime(lastTime2, currTime2));
				lastTime2 = currTime2;
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

			BufferedWriter out = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/coset300k.txt"));
			Set<Integer> keyset = coSetMapArray.keySet();
			if((coSetMapArray != null )&& (termBimap != null)){
				for(Integer c : keyset){
					ArrayList<CoWeight> cwList = coSetMapArray.get(c);
					StringBuffer sb = new StringBuffer();
					int docCount = termMatrix.get(c).size();
					if(docCount > 5){
						sb.append(termBimap.inverse().get(c) + " [" + docCount + "]" + " { ");
						boolean createLine=false;
						for(CoWeight cw: cwList){
							if(cw.correlate > 0.3){
								createLine = true;
								sb.append(termBimap.inverse().get(cw.termId)+ ": " + cw.correlate + ", ");						
							}
						}
						if(createLine){
							out.write(sb.append("\n").toString());
						}
					}
				}
				out.close();
			}
			System.out.print("asd");
		}catch(Exception ex){
			System.out.print("asd");
		}
	}
}
