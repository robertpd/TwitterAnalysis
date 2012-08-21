package com.twitter.corpus.analysis;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.twitter.corpus.types.CoWeight;

public class CosetSerializer {
	private static final Logger LOG = Logger.getLogger(CosetSerializer.class);
	public static void cosetSerializer(HashMap<Integer, ArrayList<CoWeight>> blockCoSet, String output, int count, int index, double m) throws IOException{
		LOG.info("Serializing Coset: " + String.valueOf(count) + ", m = " + String.valueOf(m));
		String path = output + "/" + String.valueOf(index) + "_" + String.valueOf(m) + "_termCoset_" + count + ".ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(blockCoSet);
		objectOut.close();
		LOG.info("Finished.");
	}
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
		LOG.info("Serializing corpus coset.");
		String path = output + "/termCoset.ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(corpusCoSetArray);
		objectOut.close();
		LOG.info("Finished.");
	}
}