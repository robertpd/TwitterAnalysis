package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.twitter.corpus.types.Serialization2;

public class testttt {

	public static void main(String args[]) throws IOException{
		String tfMapPath = "/home/dock/Documents/IR/AmazonResults/mRange3/ds/tfMap.ser";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/ds/";
		HashMap<Integer, Integer> tfMap = Serialization2.deserialize(tfMapPath);
		
		BufferedWriter bw = new BufferedWriter( new FileWriter(output + "tfmap " + tfMap.size() + ".csv" ));
		
		Iterator<Entry<Integer, Integer>> asd = tfMap.entrySet().iterator();
				
				while(asd.hasNext()){
					Entry<Integer, Integer> entry = asd.next();
					bw.append(entry.getKey() + "," + entry.getValue()+"\n");
				}
				bw.close();
		
	}
}