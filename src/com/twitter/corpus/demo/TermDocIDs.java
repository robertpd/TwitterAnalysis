package com.twitter.corpus.demo;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.text.Normalizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.DefaultSimilarity;

import com.twitter.corpus.data.HtmlStatusBlockReader;
import com.twitter.corpus.data.HtmlStatusCorpusReader;
import com.twitter.corpus.data.JsonStatusBlockReader;
import com.twitter.corpus.data.JsonStatusCorpusReader;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;

	public class TermDocIDs {
		private static final Logger LOG = Logger.getLogger(IndexStatuses.class);

		public TermDocIDs() {}

		public static enum HashField {
			HASH("hash"),
			MENTION("mention"),
			CREATED_AT("created_at"),
			ID("id"),
			SCREEN_NAME("screen_name"),
			TEXT("text");

			public final String name;
			HashField(String s){
				name = s;
			}
		};

		private static final String INPUT_OPTION = "input";
		private static final String INDEX_OPTION = "index";

		private static final String HTML_MODE = "html";
		private static final String JSON_MODE = "json";

		private static final String hash = "#";
		private static final String mention = "@";
		
		private static Set<String> stopwords;
		private static String stopsFile[] = {"/home/dock/Documents/IR/DataSets/stopwords/english",
											 "/home/dock/Documents/IR/DataSets/stopwords/misc",
											 "/home/dock/Documents/IR/DataSets/stopwords/french",
											 "/home/dock/Documents/IR/DataSets/stopwords/english",
											 "/home/dock/Documents/IR/DataSets/stopwords/portuguese",
											 "/home/dock/Documents/IR/DataSets/stopwords/non_eng_stops",
											 "/home/dock/Documents/IR/DataSets/stopwords/portu_stops",
											 "/home/dock/Documents/IR/DataSets/stopwords/spanish_stops",
											};
		
		private ArrayList<String> hashList;
		private ArrayList<String> mentionList;
		private ArrayList<String> termList;
		private ArrayList<String> bigramList;
		

		@SuppressWarnings("static-access")
		public static void main(String[] args) throws Exception {
			TermDocIDs td = new TermDocIDs();
			System.out.println("Classpath = " + System.getProperty("java.class.path"));
			
			Options options = new Options();
			options.addOption(OptionBuilder.withArgName("path").hasArg()
					.withDescription("input directory or file").create(INPUT_OPTION));
			options.addOption(OptionBuilder.withArgName("path").hasArg()
					.withDescription("index location").create(INDEX_OPTION));
			options.addOption(HTML_MODE, false, "input is HTML SequenceFile; mutually exclusive with -" + JSON_MODE);
			options.addOption(JSON_MODE, false, "input is JSON; mutually exclusive with -" + HTML_MODE);

			CommandLine cmdline = null;
			CommandLineParser parser = new GnuParser();
			try {
				cmdline = parser.parse(options, args);
			} catch (ParseException exp) {
				System.err.println("Error parsing command line: " + exp.getMessage());
				System.exit(-1);
			}

			if (!(cmdline.hasOption(INPUT_OPTION) && cmdline.hasOption(INDEX_OPTION) && (cmdline.hasOption(HTML_MODE)))) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(IndexStatuses.class.getName(), options);
				System.exit(-1);
			}

			File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));

			LOG.info("Indexing " + cmdline.getOptionValue(INPUT_OPTION));
			StatusStream stream = null;
			// Figure out if we're reading from HTML SequenceFiles or JSON.
			if (cmdline.hasOption(HTML_MODE)) {
				FileSystem fs = FileSystem.get(new Configuration());
				Path file = new Path(cmdline.getOptionValue(INPUT_OPTION));
				if (!fs.exists(file)) {
					System.err.println("Error: " + file + " does not exist!");
					System.exit(-1);
				}

				if (fs.getFileStatus(file).isDir()) {
					stream = new HtmlStatusCorpusReader(file, fs);
				} else {
					stream = new HtmlStatusBlockReader(file, fs);
				}
			}else {
				File file = new File(cmdline.getOptionValue(INPUT_OPTION));
				if (!file.exists()) {
					System.err.println("Error: " + file + " does not exist!");
					System.exit(-1);
				}

				if (file.isDirectory()) {
					stream = new JsonStatusCorpusReader(file);
				} else {
					stream = new JsonStatusBlockReader(file);
				}
			}

			int cnt = 0;
			Status status;
//			BufferedWriter hashOut = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/Dump/hashOut.txt"));
			HashMap<String, ArrayList<Long>> hashPostings = new HashMap<String, ArrayList<Long>>();
			
			HashMap<String, ArrayList<Long>> termPostings = new HashMap<String, ArrayList<Long>>();
			HashMap<String, Integer> termFreq = new HashMap<String, Integer>();
			
			HashMap<String, ArrayList<Long>> bigramPostings = new HashMap<String, ArrayList<Long>>();
			HashMap<String, Integer> bigramFreq = new HashMap<String, Integer>();
			
			HashMap<String, ArrayList<Long>> mentionPostings = new HashMap<String, ArrayList<Long>>();
			
			stopwords = readFile(stopsFile);
			
			try 
			{
//				int c=0;
				while ((status = stream.next()) != null) 
				{
					if (status.getText() == null) 
					{
						continue;
					}
					ProcessedTweet pt = td.processTweet(status.getText());
					
					// 1.	Add elements to ArrayList
					// get hash list, add hash to map with list of docids
//					String hash;
//					while(pt.hashList.iterator().hasNext()){
//						hash = pt.hashList.iterator().next();
//						if(!hashPostings.containsKey(hash)){
//							ArrayList<Long> postings = new ArrayList<Long>();
//							postings.add(status.getId());
//							hashPostings.put(hash, postings);
//						}
//						else{
//							hashPostings.get(hash).add(status.getId());
//							System.gc();
//						}
//					}
					
					// terms
					
					for(int i=0 ; i < pt.termList.size(); i++){
						if(!termFreq.containsKey(pt.termList.get(i))){
							termFreq.put(pt.termList.get(i), 1);
						}
						else{
							termFreq.put(pt.termList.get(i), termFreq.get(pt.termList.get(i))+1);
						}
//						if(!termPostings.containsKey(pt.termList.get(i))){
//							ArrayList<Long> postings = new ArrayList<Long>();
//							postings.add(status.getId());
//							termPostings.put(pt.termList.get(i), postings);
//						}
//						else{
//							termPostings.get(pt.termList.get(i)).add(status.getId());
//							System.gc();
//						}
					}
					
					// bigrams
					for(int i=0 ; i < pt.bigramList.size(); i++){
						if(!bigramFreq.containsKey(pt.bigramList.get(i))){
							bigramFreq.put(pt.bigramList.get(i), 1);
						}
						else{
							bigramFreq.put(pt.bigramList.get(i), bigramFreq.get(pt.bigramList.get(i))+1);
						}
//						if(!bigramPostings.containsKey(pt.bigramList.get(i))){
//							ArrayList<Long> postings = new ArrayList<Long>();
//							postings.add(status.getId());
//							bigramPostings.put(pt.bigramList.get(i), postings);
//						}
//						else{
//							bigramPostings.get(pt.bigramList.get(i)).add(status.getId());
//							System.gc();
//						}
					}
//					if(cnt % 10 == 0){
//						Thread.currentThread().sleep(2000);
//					}
										
					// mentions
//					for(int i=0 ; i < pt.mentionList.size(); i++){
//						if(!mentionPostings.containsKey(pt.mentionList.get(i))){
//							ArrayList<Long> postings = new ArrayList<Long>();
//							postings.add(status.getId());
//							mentionPostings.put(pt.mentionList.get(i), postings);
//						}
//						else{
//							mentionPostings.get(pt.mentionList.get(i)).add(status.getId());
//							System.gc();
//						}
//					}
					
//					if(cnt % 10 == 0){
//						Thread.currentThread().sleep(2000);
//					}
					cnt++;
					if (cnt % 10000 == 0) 
					{
						LOG.info(cnt + " statuses indexed");
						System.gc();
					}
//					if(cnt % 10 == 0){
//						Thread.currentThread().sleep(3000);
//					}
//					c++;
//					if(c==1000){
//						break;
//					}
				}
				// 1. sort doc ids
				// 2. print to text file
				
				//	this sorting should only be done everyso often ....heap size prob trips here
//				Set<String> hashKeys = hashPostings.keySet();
//				for(String key : hashKeys){
//					Collections.sort(hashPostings.get(key));
//				}
//				Set<String> mentionKeys = mentionPostings.keySet();
//				for(String key : mentionKeys){
//					Collections.sort(mentionPostings.get(key));
//				}
//				Set<String> termKeys = termPostings.keySet();
//				for(String key : termKeys){
//					Collections.sort(termPostings.get(key));
//				}
//				Set<String> bigramKeys = bigramPostings.keySet();
//				for(String key : bigramKeys){
//					Collections.sort(bigramPostings.get(key));
//				}
				
				FileOutputStream termFreqFile = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/freqs/termFreq.ser");
				ObjectOutputStream termFreqOut = new ObjectOutputStream(termFreqFile);
				termFreqOut.flush();
				termFreqOut.writeObject(termFreq);
				termFreqOut.close();
				
				FileOutputStream bigramFreqFile = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/freqs/bigramFreq.ser");
				ObjectOutputStream bigramFreqOut = new ObjectOutputStream(bigramFreqFile);
				bigramFreqOut.flush();
				bigramFreqOut.writeObject(bigramFreq);
				bigramFreqOut.close();
				
//				if(inFile.exists()){
//					try{
//						ObjectInputStream ois = new ObjectInputStream(new FileInputStream(inFile));
//						retVal = (TermTrend[]) ois.readObject();
//						ois.close(); 
//					}
//					catch(Exception e){}
//				}
//				else{
//					TermDayTrend trends = new TermDayTrend();
//					retVal = trends.getTrend(reader, rankedTerms);
//					try {
//						if(retVal != null){
//						FileOutputStream fileOut = new FileOutputStream("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/index__created_at_un_an/trended_terms.ser");
//						ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
//						objectOut.flush();
//						objectOut.writeObject(retVal);
//						objectOut.close();
//						}
//					}
//					catch(Exception e){
//						if(inFile.exists()){
//							inFile.delete();
//						}
//						e.printStackTrace();
//					}
//				}
				
				LOG.info("Optimizing index...");
			} finally 
			{
				
			}
			LOG.info(String.format("Total of %s statuses indexed", cnt));

		}
		// method gets n-grams
		private ProcessedTweet processTweet(String s){
			ProcessedTweet pt = new ProcessedTweet();
			String[] prepTweet = preprocessTweet(s);

			//	collect hash's and mentions
			for(String token : prepTweet){
				if(token.startsWith(hash, 0)){
					pt.hashList.add(token);
				}
				if(token.startsWith(mention, 0)){
					pt.mentionList.add(token);
				}
			}
			// strip stopwords
			//	get n-grams
			for (int i = 0; i < prepTweet.length; i++) {
				if(!stopwords.contains(prepTweet[i])){
					if(prepTweet[i].length() > 0){
						if (i == prepTweet.length - 1) {
							pt.termList.add(prepTweet[i]);
						} else {
							pt.termList.add(prepTweet[i]);
							if(!stopwords.contains(prepTweet[i+1])){
								pt.bigramList.add(prepTweet[i] + " " + prepTweet[i+1]);
							}
						}
					}
				}
			}
			return pt;
		}
		public static void callStopwords() throws IOException{
			stopwords = readFile(stopsFile);
		}
		public static String[] removeStops(String[] tweet){
			// remove stops: 	add to array list, convert arl to string[]
			String[] retVal = new String[tweet.length];
			for(int i=0; i< tweet.length;i++){
				if(!stopwords.contains(tweet[i])){
					retVal[i]=tweet[i];
				}
			}
			return retVal;
		}
		public static String[] preprocessTweet(String tweet){
			// lowercase
			// split
			// normalize characters
			// tokenize
			
			String[] normElements = normalize(tweet.toLowerCase().split(" "));
			String[] retVal = new String[normElements.length];
			for(int i=0; i< normElements.length;i++){
				retVal[i]=normElements[i].replaceAll("[^A-Za-z0-9@#]","");
			}
			return retVal;
		}
		public static class ConstantNormSimilarity extends DefaultSimilarity {
			private static final long serialVersionUID = 2737920231537795826L;

			@Override
			public float computeNorm(String field, FieldInvertState state) {
				return 1.0f;
			}
		}
		
		public static Set<String> readFile(String[] paths) throws IOException {
			Set<String> stopWords = new LinkedHashSet<String>();

			for(String path : paths){
				FileInputStream stream = new FileInputStream(new File(path));
				try {
					FileChannel fc = stream.getChannel();
					MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
					/* Instead of using default, pass in a decoder. */
					String x = Charset.defaultCharset().decode(bb).toString().replace("\t", "");
					String y = x.replace("\n", " ");
					String[] a = y.split(" ");

					for(int i=0; i<a.length;i++){
						stopWords.add(a[i].replaceAll("[^A-Za-z]",""));
					}
				}
				finally {
					stream.close();
				}
			}
			return stopWords;
		}
		private static String[] normalize(String[] str){
			String[] retVal = new String[str.length];
			for(int i=0;i<str.length;i++){
				String norm = str[i];
				norm=norm.replaceAll("[éèêë]","e");
				norm=norm.replaceAll("[ûù]","u");
				norm=norm.replaceAll("[ïî]","i");
				norm=norm.replaceAll("[áàâ]","a");
				norm=norm.replaceAll("[ó]","o");
				retVal[i]=norm;
			}
			return retVal;
			//		    s = s.replaceAll("[ÈÉÊË]","E");
			//		    s = s.replaceAll("[ÛÙ]","U");
			//		    s = s.replaceAll("[ÏÎ]","I");
			//		    s = s.replaceAll("[ÀÂ]","A");
			//		    s = s.replaceAll("Ô","O");
		}
	}

