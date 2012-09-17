package com.twitter.corpus.types;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

public class Serialization2 {
	private static final Logger LOG = Logger.getLogger(Serialization2.class);
	
	/**
	 * Generic method to serialize object to path
	 * @param path
	 * @param objectToSer
	 * @throws IOException
	 */
	public static <T> void serialize(T objectToSer, String path) throws IOException{
		LOG.info("Serializing...");
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
		objectOut.flush();
		objectOut.writeObject(objectToSer);
		objectOut.close();
		LOG.info("Finished serializing");
	}
	/**
	 * Generic method to deserialize objects at path
	 * @param path
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T deserialize(String path){
		LOG.info("deserializing...");
		File cosetFile = new File(path);

		T retVal = null;
		try{			
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(cosetFile));
			retVal = (T) in.readObject();
			in.close();
		}
		catch(Exception ex){
			// gulp
		}
		LOG.info("Finished deserializing " + retVal.getClass().getName());
		return retVal;
	}
	public static ArrayList<Integer> readFiletoArray(String path) throws IOException {
		ArrayList<Integer> terms = new ArrayList<Integer>();

		FileInputStream stream = new FileInputStream(new File(path));
		try {
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
			/* Instead of using default, pass in a decoder. */
			String x = Charset.defaultCharset().decode(bb).toString().replace("\t", "");
			String y = x.replace("\n", " ");
			String[] a = y.split(" ");

			for(int i=0; i<a.length;i++){
				terms.add(Integer.parseInt(a[i]));
			}
		}
		finally {
			stream.close();
		}
		return terms;
	}
	public static Set<Integer> readFile(String path) throws IOException {
		Set<Integer> terms = new HashSet<Integer>();

		FileInputStream stream = new FileInputStream(new File(path));
		try {
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
			/* Instead of using default, pass in a decoder. */
			String x = Charset.defaultCharset().decode(bb).toString().replace("\t", "");
			String y = x.replace("\n", " ");
			String[] a = y.split(" ");

			for(int i=0; i<a.length;i++){
				terms.add(Integer.parseInt(a[i]));
			}
		}
		finally {
			stream.close();
		}
		return terms;
	}
}