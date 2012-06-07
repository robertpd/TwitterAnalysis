package com.twitter.corpus.types;

import java.util.HashMap;
import java.util.HashSet;
/**
 * 
 * Wrap the index with a total document count
 *
 */
public class IndexAndDocCount {
	public IndexAndDocCount(int Docs, HashMap<Integer, HashSet<Long>> DocColl){
		this.docCount = Docs;
		this.docColl = DocColl;
	}
	public int docCount;
	public HashMap<Integer, HashSet<Long>> docColl;
}
