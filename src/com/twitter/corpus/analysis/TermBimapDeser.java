package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;

public class TermBimapDeser {
	private static final Logger LOG = Logger.getLogger(TermBimapDeser.class);

	public static void main(String[] args) throws IOException{
		LOG.info("Doing termbimap deser.");
		String path = "/home/dock/Documents/IR/AmazonResults/mRange3/termbimap.ser";
		String termPath = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/350.csv";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/350Strings.csv";
		
		Set<Integer> termDecode = readFile(termPath);
		HashBiMap<String, Integer> termBimap = deserTermBiMap(path);
		LOG.info("finished deserializing termBimap");
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
		
		Iterator<Integer> termsDIterator = termDecode.iterator();
		while(termsDIterator.hasNext()){
			Integer t = termsDIterator.next();
			bw.append(termBimap.inverse().get(t) + "\n");
		}
		bw.close();
	}
	@SuppressWarnings("unchecked")
	private static HashBiMap<String, Integer> deserTermBiMap(String path){
		File bimapFile = new File(path);

		HashBiMap<String, Integer> retVal = null;
		try{			
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(bimapFile));
			retVal = (HashBiMap<String, Integer>) in.readObject();
			in.close();
		}
		catch(Exception ex){
			// gulp
		}
		return retVal;
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
