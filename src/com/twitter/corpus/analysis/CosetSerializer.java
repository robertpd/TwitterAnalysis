package com.twitter.corpus.analysis;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import com.twitter.corpus.types.CoWeight;

public class CosetSerializer {

	public static void cosetSerializer(HashMap<Integer, ArrayList<CoWeight>> blockCoSet, String output, int count, int index, double m) throws IOException{

		String path = output + "/" + String.valueOf(index) + "_" + String.valueOf(m) + "_termCoset_" + count + ".ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(blockCoSet);
		objectOut.close();
	}
	public static void cosetSerializer(HashMap<Integer, ArrayList<CoWeight>> blockCoSet, String output, int count) throws IOException{

		String path = output + "/termCoset_" + count + ".ser";
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(blockCoSet);
		objectOut.close();
	}
}