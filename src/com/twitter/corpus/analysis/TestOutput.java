package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class TestOutput {

	/**
	 * Provide Generic Map printing method
	 * @param mapp
	 * @param output
	 * @throws IOException
	 */
	public static <T, K> void printMap(HashMap<T,K> mapp, String output) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
		bw.flush();
		Iterator<Map.Entry<T,K>> mapIter =  mapp.entrySet().iterator();
		while(mapIter.hasNext()){
			Entry<T, K> entry = mapIter.next();
			bw.append(entry.getKey().toString());
			bw.append("\t");
			bw.append(entry.getValue().toString());
			bw.append("\n");
		}
		bw.close();
	}
}
