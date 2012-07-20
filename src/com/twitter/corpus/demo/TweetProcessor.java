package com.twitter.corpus.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;

import com.twitter.corpus.analysis.Stemmer;
import com.twitter.corpus.analysis.TermTermWeights;

public class TweetProcessor {
	public static Set<String> stopwords = null;
	public static int uniqueTermCounter=0;
	//	private static String stopsFile[] = {"/home/dock/Documents/IR/DataSets/stopwords/english",
	//		"/home/dock/Documents/IR/DataSets/stopwords/misc",
	//		"/home/dock/Documents/IR/DataSets/stopwords/french",
	//		"/home/dock/Documents/IR/DataSets/stopwords/english",
	//		"/home/dock/Documents/IR/DataSets/stopwords/portuguese",
	//		"/home/dock/Documents/IR/DataSets/stopwords/non_eng_stops",
	//		"/home/dock/Documents/IR/DataSets/stopwords/portu_stops",
	//		"/home/dock/Documents/IR/DataSets/stopwords/spanish_stops",
	//		"/home/dock/Documents/IR/DataSets/stopwords/analysis",
	//	};
	private static String stopDir = "stopwords";

	private static String stopsFile[] = {
		TweetAnalysis.toolsDir + "/" + stopDir + "/english",
		TweetAnalysis.toolsDir + "/" + stopDir + "/misc",
		TweetAnalysis.toolsDir + "/" + stopDir + "/french",
		TweetAnalysis.toolsDir + "/" + stopDir + "/english",
		TweetAnalysis.toolsDir + "/" + stopDir + "/portuguese",
		TweetAnalysis.toolsDir + "/" + stopDir + "/non_eng_stops",
		TweetAnalysis.toolsDir + "/" + stopDir + "/portu_stops",
		TweetAnalysis.toolsDir + "/" + stopDir + "/spanish_stops",
		TweetAnalysis.toolsDir + "/" + stopDir + "/analysis",
	};

	public static void callStops() throws IOException{
		if(stopwords == null){
			stopwords = readFile(stopsFile);
		}
	}

	public static ProcessedTweet processTweet(String s, long docId){
		// TODO Strip retweets
		ProcessedTweet pt = new ProcessedTweet();
		String[] prepTweet = preprocessTweet(s);
		//	collect hash's and mentions
		for(String token : prepTweet){
			if(token.startsWith("#", 0)){
				pt.hashList.add(token);
			}
			if(token.startsWith("@", 0)){
				pt.mentionList.add(token);
			}
		}
		// strip stopwords, get ngrams
		ArrayList<Integer> termsInDoc = new ArrayList<Integer>();
		for (int i = 0; i < prepTweet.length; i++) {
			if(!stopwords.contains(prepTweet[i])){
				if(prepTweet[i].length() > 1 && !prepTweet[i].startsWith("@")){
					if(!TermTermWeights.termBimap.containsKey(prepTweet[i])){
						TermTermWeights.termBimap.put(prepTweet[i], uniqueTermCounter);
						uniqueTermCounter++;
					}

					// blanked for termbimap
					int termId = TermTermWeights.termBimap.get(prepTweet[i]);
					pt.termIdList.add(termId);
					termsInDoc.add(termId);
				}
			}
		}
		// blanked for termbimap
		if(termsInDoc.size() > 0){	// discarding non alphanumeric chars like asian chars => empty termsInDoc
			TermTermWeights.docTermsMap.put(docId, termsInDoc);
		}
		return pt;
	}
	public static String[] preprocessTweet(String tweet){
		// lowercase
		// split
		// normalize characters
		// tokenize
		// stem
		String[] normElements = normalize(tweet.toLowerCase().split(" "));
		String[] retVal = new String[normElements.length];

		// http link processing removed, 730k tweets contain 112k http links
		//		if(tweet.contains("http")){
		//			for(int i=0; i< normElements.length;i++){
		//				retVal[i]=normElements[i].contains("http") ? normElements[i] :normElements[i].replaceAll("[^A-Za-z0-9@#]","");
		//			} 
		//		}
		//		else{
		for(int i=0; i< normElements.length;i++){
			retVal[i]=normElements[i].replaceAll("[^A-Za-z0-9@#]","");
		}
		//		}
		return retVal;
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
			retVal[i] = Stemmer.stemTerm(norm);
		}
		return retVal;
	}
	private static Set<String> readFile(String[] paths) throws IOException {
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
}
