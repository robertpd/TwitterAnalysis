package com.twitter.corpus.types;

import java.util.ArrayList;

/**
 * Structure to manage data generated by a processed tweet
 * @author dock
 *
 */
public class ProcessedTweet {

	public ProcessedTweet(){
		hashList = new ArrayList<String>(2);	//avg
		mentionList = new ArrayList<String>(2);	//avg
		termIdList = new ArrayList<Integer>(8);	//avg term count is ~8 
	}
	
	public ArrayList<Integer> termIdList;
	public ArrayList<String> hashList;
	public ArrayList<String> mentionList;
	public ArrayList<String> termList;
}
