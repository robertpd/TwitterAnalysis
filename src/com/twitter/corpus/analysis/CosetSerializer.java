package com.twitter.corpus.analysis;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;

public class CosetSerializer {

	public static void cosetSerializer(HashMap<Integer, HashMap<Integer, Double>> coset , String output) throws IOException{
		String path = output + "/termCoset" +3 +".ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(coset);
		objectOut.close();
	}
}
