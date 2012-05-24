package com.twitter.corpus.demo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.twitter.corpus.analysis.TermTermWeights;
import com.twitter.corpus.data.HtmlStatusCorpusReader;
import com.twitter.corpus.data.StatusStream;
import com.twitter.corpus.types.CoWeight;

public class CorpusExtractor2{
	private static final Logger LOG = Logger.getLogger(IndexStatuses.class);
	private CorpusExtractor2() {}
	//	public static HashMap<Integer, Integer> termUniqueness;
	public static ArrayList<UniquePairs> termUniqueness;

	//	private static final String INPUT_OPTION = "input";
	//	private static final String INDEX_OPTION = "index";
	//	private static final String HTML_MODE = "html";
	//	private static final String JSON_MODE = "json";

	public static void main(String[] args) throws Exception {
		System.out.println("Classpath = " + System.getProperty("java.class.path"));

		String root = "/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/html/20110";
		String[] filePaths = {root + "123", root + "124", root + "125", root + "126", root + "127", root + "128", root + "129", root + "130", root + "131", 
				root + "201", root + "202", root + "203", root + "204", root + "205", root + "206", root + "207", root + "208"};
		//		File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));

		// Figure out if we're reading from HTML SequenceFiles or JSON.
		int cnt=0;
		HashMap<Integer, ArrayList<CoWeight>> blockCoSet = null;
		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corpusCoSetArray = new ArrayList<HashMap<Integer,ArrayList<CoWeight>>>();
		for(String path : filePaths){
			LOG.info("Indexing " + filePaths[cnt]);
			StatusStream stream = null;
			FileSystem fs = FileSystem.get(new Configuration());
			Path file = new Path(filePaths[cnt]);
			if (!fs.exists(file)) {
				System.err.println("Error: " + file + " does not exist!");
				System.exit(-1);
			}
			File dir = new File(path);
			File[] files = dir.listFiles();

			if (fs.getFileStatus(file).isDir()) {
				stream = new HtmlStatusCorpusReader(file, fs);
			}
			TermTermWeights ill = new TermTermWeights(stream);
			blockCoSet = ill.Index();
			corpusCoSetArray.add(blockCoSet);
			cnt++;
			//			Thread.currentThread();
			//			Thread.sleep(20000);
		}

		try{
			if((corpusCoSetArray != null )&& (TermTermWeights.termBimap != null)){
				Set<Integer> allKeys = new HashSet<Integer>();
				for(int day = 0; day < corpusCoSetArray.size()-1; day++){
					allKeys.addAll(corpusCoSetArray.get(day).keySet());						
				}
				BufferedWriter trendOut = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/range.txt"));

				Iterator<Integer> termKeysIter = allKeys.iterator();
				//				termUniqueness = new HashMap<Integer, Integer>(100);
				termUniqueness = new ArrayList<CorpusExtractor2.UniquePairs>(100);
				while(termKeysIter.hasNext()){
					int term = termKeysIter.next();
					int skip=0;
					for(int i=0; i < corpusCoSetArray.size(); i++){
						ArrayList<CoWeight> coWeightArrayList = corpusCoSetArray.get(i).get(term);
						if(coWeightArrayList == null || coWeightArrayList.size()==0)
							skip ++;
					}
					if(skip>10){
						continue;
					}
					Set<Integer> uniques = new HashSet<Integer>();
					for(int i=0; i < corpusCoSetArray.size(); i++){
						ArrayList<CoWeight> coWeightArrayList = corpusCoSetArray.get(i).get(term);
						if(coWeightArrayList!=null){
							short skipCount=0;
							for(CoWeight cw : coWeightArrayList){
								if(skipCount==5)
									break;
								if(cw != null){
									uniques.add(cw.termId);
									skipCount++;
								}
							}
						}
					}
					UniquePairs up = new UniquePairs(term, uniques.size());
					termUniqueness.add(up);
				}
				trendOut.close();

				Collections.sort(termUniqueness, new UniquenessComparator());
				int perc=0;
				HashMap<Integer, Double> asd = new HashMap<Integer, Double>(100);
				for(int i=0; i< termUniqueness.size()-1 ; i++){
					if(termUniqueness.get(i).uniqueCount == termUniqueness.get(i+1).uniqueCount){
						perc++;
					}
					else if(termUniqueness.get(i).uniqueCount < termUniqueness.get(i+1).uniqueCount){
						double leval = (double)(perc*100)/(double)termUniqueness.size();
						leval = (double)Math.round(leval * 1000) / 1000;
						asd.put(termUniqueness.get(i).uniqueCount, leval);
						perc=0;
					}
				}
				System.out.print("finito");
			}
		}
		catch(Exception ex){
			System.out.print("asd");
		}
		try{
			HashSet<Integer> termCheck = null;
			if((corpusCoSetArray != null )&& (TermTermWeights.termBimap != null)){
				Set<Integer> allKeys = new HashSet<Integer>();
				for(int day = 0; day < corpusCoSetArray.size()-1; day++){
					allKeys.addAll(corpusCoSetArray.get(day).keySet());						
				}
				BufferedWriter out = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/all.txt"));

				Iterator<Integer> termKeysIter = allKeys.iterator();
				while(termKeysIter.hasNext()){
					Integer term = termKeysIter.next();
					int skip=0;
					for(int i=0; i < corpusCoSetArray.size(); i++){
						ArrayList<CoWeight> coWeightArrayList = corpusCoSetArray.get(i).get(term);
						if(coWeightArrayList == null)
							skip ++;
					}
					if(skip>10){
						continue;
					}
					for(int i=0; i < corpusCoSetArray.size(); i++){
						ArrayList<CoWeight> coWeightArrayList = corpusCoSetArray.get(i).get(term);

						// want to block all print outs unless theres is data for 15 out of 17 days
						StringBuffer sb = new StringBuffer();
						if(coWeightArrayList==null){
							out.append("Term: "+TermTermWeights.termBimap.inverse().get(term)+", Day: "+(i+1)+": { NULL }\n");
						}
						else{
							boolean newLine = false;
							boolean beginLine = true;
							short printLimit = 0;
							for(CoWeight cw : coWeightArrayList){
								if(cw == null){
									sb.append("Term: "+TermTermWeights.termBimap.inverse().get(term)+", Day: "+(i+1)+" { NULL }\n");
								}
								else{
									if(beginLine){
										sb.append("Term: "+TermTermWeights.termBimap.inverse().get(term)+", Day : "+(i+1)+" { ");
										beginLine = false;
									}
									//									if(cw.correlate > 0.2){
									if(printLimit < 5){
										newLine = true;
										sb.append(TermTermWeights.termBimap.inverse().get(cw.termId)+ ": " + cw.correlate + ", ");
										printLimit++;
									}
									//									}
								}
							}
							if(newLine){
								out.write(sb.replace(sb.length()-2, sb.length()-2,"").append(" }\n").toString());
							}
						}
					}
				}
				out.close();
			}
			System.out.print("finito");
		}catch(Exception ex){
			System.out.print("asd");
		}
	}
	public static class UniquenessComparator implements Comparator<UniquePairs> {
		@Override
		public int compare(UniquePairs arg0, UniquePairs arg1) {
			return arg0.uniqueCount.compareTo(arg1.uniqueCount);
		}
	}
	public static class UniquePairs{
		public UniquePairs(int a, int b){
			term = a;
			uniqueCount = b;
		}
		public int term;
		public Integer uniqueCount;
	}
}