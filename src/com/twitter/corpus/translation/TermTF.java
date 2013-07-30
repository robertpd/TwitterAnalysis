package com.twitter.corpus.translation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.twitter.corpus.types.SerializationHelper;


/** 
 * calculate term frequencies and write to disk 
 * 
 *
 */
public class TermTF {
	public static void main(String[] args) throws IOException{
	
		String pathToDecode = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/528/0.3 terms.csv";
		String tfMapPath = "/home/dock/Documents/IR/AmazonResults/mRange3/tfMap.ser";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/528/0.3 tfs.csv";
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
		HashSet<Integer> termSet =  (HashSet<Integer>) SerializationHelper.readFile(pathToDecode);
		HashMap<Integer, Integer> tfMap = deser(tfMapPath);
		Iterator<Integer> termIter = termSet.iterator();
		while(termIter.hasNext()){
			Integer entry = termIter.next();
			bw.append(entry.toString() + "," + tfMap.get(entry).toString() + "\n");
		}
		bw.close();
	}
	@SuppressWarnings("unchecked")
	public static HashMap<Integer, Integer> deser(String path){
		HashMap<Integer, Integer> retVal = null;
		File tfMapFile = new File(path);
		try{			
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(tfMapFile));
			retVal = (HashMap<Integer, Integer>) in.readObject();
			in.close();
		}
		catch(Exception ex){
		}
		return retVal;
	}
}
