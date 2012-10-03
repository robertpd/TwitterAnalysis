package com.twitter.corpus.analysis;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.twitter.corpus.types.CoWeight;
import com.twitter.corpus.types.Serialization2;

public class CosetSerializer {
	private static final Logger LOG = Logger.getLogger(CosetSerializer.class);
	public static void cosetSerializer(HashMap<Integer, ArrayList<CoWeight>> blockCoSet, String output, int count) throws IOException{
		LOG.info("Serializing Coset: " + String.valueOf(count));
		String path = output + "/termCoset_" + count + ".ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(blockCoSet);
		objectOut.close();
		LOG.info("Finished.");
	}
	public static void copusCosetSer(ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corpusCoSetArray, String output) throws IOException{
		Serialization2.serialize(corpusCoSetArray, output + "/corpusTermCoset.ser");

	}
}