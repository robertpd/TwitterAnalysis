package com.twitter.corpus.demo;

import java.util.ArrayList;

public class ProcessedTweet {

	public ProcessedTweet(){
		hashList = new ArrayList<String>();
		mentionList = new ArrayList<String>();
//		termList = new ArrayList<String>();
		termIdList = new ArrayList<Integer>();
//		bigramList = new ArrayList<String>();
	}
	
	public ArrayList<Integer> termIdList;
	public ArrayList<String> hashList;
	public ArrayList<String> mentionList;
	public ArrayList<String> termList;
//	public ArrayList<String> bigramList;
}
