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

import com.twitter.corpus.analysis.InvertedIndex;
import com.twitter.corpus.analysis.OutTermCosets;
import com.twitter.corpus.analysis.TermTermWeights;
import com.twitter.corpus.data.HtmlStatusCorpusReader;
import com.twitter.corpus.data.StatusStream;
import com.twitter.corpus.types.CoWeight;

public class CorpusExtractor2{
	private static final Logger LOG = Logger.getLogger(IndexStatuses.class);
	private CorpusExtractor2() {}
	public static ArrayList<UniquePairs> termUniqueness;
	//	private static final String INPUT_OPTION = "input";
	//	private static final String INDEX_OPTION = "index";
	//	private static final String HTML_MODE = "html";
	//	private static final String JSON_MODE = "json";

	public static void main(String[] args) throws Exception {
		System.out.println("Classpath = " + System.getProperty("java.class.path"));

		String root = "/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/html/20110";
		String[] filePaths = {root + "123", root + "124", root + "125", root + "126", root + "127", root + "128",
							  root + "129", root + "130", root + "131", root + "201", root + "202", root + "203",
							  root + "204", root + "205", root + "206",	root + "207", root + "208"};
//		String[] filePaths = {root + "123", root + "123a", root + "124", root + "124a", root + "125", root + "125a", 
//				  root + "126", root + "126a", root + "127", root + "127a", root + "128", root + "128a",
//				  root + "129", root + "129a", root + "130", root + "130a", root + "131", root + "131a",
//				  root + "201", root + "201a", root + "202", root + "202a", root + "203", root + "203a",
//				  root + "204", root + "204a", root + "205", root + "205a", root + "206", root + "206a", 
//				  root + "207", root + "207a", root + "208", root + "208a"};
		//		File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));

//		int cnt=0;
		HashMap<Integer, ArrayList<CoWeight>> blockCoSet = null;
		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corpusCoSetArray = new ArrayList<HashMap<Integer,ArrayList<CoWeight>>>();
		for(String path : filePaths){
			LOG.info("Indexing " + path);
			StatusStream stream = null;
			FileSystem fs = FileSystem.get(new Configuration());
			
			Path file = new Path(path);
			
			if (!fs.exists(file)) {
				System.err.println("Error: " + file + " does not exist!");
				System.exit(-1);
			}
			if (fs.getFileStatus(file).isDir()) {
				stream = new HtmlStatusCorpusReader(file, fs);
			}
			
			InvertedIndex ii = new InvertedIndex();
			HashMap<Integer, HashSet<Long>> termIndex = ii.buildIndex(stream);
			
			TermTermWeights ill = new TermTermWeights(termIndex);
			blockCoSet = ill.termCosetBuilder();
			corpusCoSetArray.add(blockCoSet);
//			cnt++;
//			Thread.currentThread();
//			Thread.sleep(20000);
		}

		// output a day by day breakdown of term co-weights, can set a threshold of terms to skip 
		// if they don't occur enough over the x day period, slightly dodgy
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
		// output term trends, with static print method. prints term with list of correlates and weight		
		OutTermCosets.print(corpusCoSetArray);
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