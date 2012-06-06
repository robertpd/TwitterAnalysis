package com.twitter.corpus.analysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;

import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;
import com.twitter.corpus.demo.Admin;
import com.twitter.corpus.demo.ProcessedTweet;
import com.twitter.corpus.demo.TweetProcessor;

public class InvertedIndex {
	public InvertedIndex(){
	}
	private static final Logger LOG = Logger.getLogger(TermTermWeights.class);
	//	public static int counter =1;

	public HashMap<Integer, HashSet<Long>> buildIndex(StatusStream stream) throws IOException{
		int skip=0;
		if(TweetProcessor.stopwords == null){
			TweetProcessor.callStops();
		}
		HashMap<Integer, HashSet<Long>> termIndex = new HashMap<Integer, HashSet<Long>>();
		int docNum=0;
		Status status;
		Long lastTime = 0l;
		try {
			while ((status = stream.next()) != null)
			{
				skip++;
				if(skip!=17){
					continue;
				}
				if(skip==17){skip =0;}
				if(status.getHttpStatusCode() == 302){
					continue;
				}					

				String tweet = status.getText();
				if (tweet == null){	continue;}
				ProcessedTweet pt = TweetProcessor.processTweet(status.getText(),status.getId());

				for(int i=0; i< pt.termIdList.size() ; i++){
					if(!termIndex.containsKey(pt.termIdList.get(i)))
					{// tdh - termDocHash
						HashSet<Long> tdh = new HashSet<Long>();
						if(!tdh.contains(status.getId())){
							tdh.add(status.getId());
						}
						termIndex.put(pt.termIdList.get(i), tdh);
					}
					else
					{
						if(!termIndex.get(pt.termIdList.get(i)).contains(status.getId())){
							termIndex.get(pt.termIdList.get(i)).add(status.getId());
						}
					}
				}
				docNum++;
				if(docNum % 10000 == 0 ){
					Long currTime = System.currentTimeMillis();
					LOG.info("block: "/*+counter+"*/ +docNum + " tweets processed in " +  Admin.getTime(lastTime, currTime));
					lastTime = currTime;
				}
				if(docNum > 50000){
					LOG.info(termIndex.size() + " total terms.");
					//					counter++;
					break;
				}
			}
		}
		finally
		{
		}
		return termIndex;
	}
}
