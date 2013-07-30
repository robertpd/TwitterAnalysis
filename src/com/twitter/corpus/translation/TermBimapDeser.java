package com.twitter.corpus.translation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.log4j.Logger;

import com.google.common.collect.HashBiMap;
import com.twitter.corpus.types.SerializationHelper;

/**
 * Deserialize the term Bimap, Provides a print out of terms represented by integers. 
 * Useful to understand what the numbers actually mean!
 * @author dock
 *
 */
public class TermBimapDeser {
	private static final Logger LOG = Logger.getLogger(TermBimapDeser.class);

	public static void main(String[] args) throws IOException{
		LOG.info("Doing termbimap deser.");
		String path = "/home/dock/Documents/IR/AmazonResults/mRange3/termbimap.ser";
		String termPath = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/528/528.csv";
		String output = "/home/dock/Documents/IR/AmazonResults/mRange3/tc_0.05/528/528Strings.csv";
		
		ArrayList<Integer> termDecode = SerializationHelper.readFiletoArray(termPath);
		HashBiMap<String, Integer> termBimap = SerializationHelper.deserialize(path);
		LOG.info("finished deserializing termBimap");
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
		
		Iterator<Integer> termsDIterator = termDecode.iterator();
		while(termsDIterator.hasNext()){
			Integer t = termsDIterator.next();
			bw.append(t + "," + termBimap.inverse().get(t) + "\n");
		}
		bw.close();
	}
}
