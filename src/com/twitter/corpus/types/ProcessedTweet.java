package com.twitter.corpus.types;

import java.util.ArrayList;

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
