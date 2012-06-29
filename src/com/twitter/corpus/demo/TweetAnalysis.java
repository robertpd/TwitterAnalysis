package com.twitter.corpus.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

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

import com.twitter.corpus.analysis.InvertedIndex;
import com.twitter.corpus.analysis.Jaccard;
import com.twitter.corpus.analysis.TermTermWeights;
import com.twitter.corpus.data.HtmlStatusCorpusReader;
import com.twitter.corpus.data.StatusStream;

public class TweetAnalysis{
	private static final Logger LOG = Logger.getLogger(IndexStatuses.class);
	public static String jaccardOutput;
	private TweetAnalysis() {}
		private static final String INPUT_OPTION = "input";
		private static final String INDEX_OPTION = "index";
	//	private static final String HTML_MODE = "html";
	//	private static final String JSON_MODE = "json";

	
	public static void main(String[] args) throws Exception {
		//		System.out.println("Classpath = " + System.getProperty("java.class.path"));
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input directory or file").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("index location").create(INDEX_OPTION));
		
		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!(cmdline.hasOption(INPUT_OPTION) && cmdline.hasOption(INDEX_OPTION) )) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(IndexStatuses.class.getName(), options);
			System.exit(-1);
		}
		
		jaccardOutput = cmdline.getOptionValue(INDEX_OPTION);
		
		String rootBase = cmdline.getOptionValue(INPUT_OPTION);
		String root = rootBase + "/20110";
//		String root = "/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/html/20110";
		String[] filePaths = {root + "123", root + "124", root + "125", root + "126", root + "127", root + "128",
				root + "129", root + "130", root + "131", root + "201", root + "202", root + "203",
				root + "204", root + "205", root + "206",	root + "207", root + "208"};
		//		String[] filePaths = {root + "123"};
		//		String[] filePaths = {root + "123", root + "123a", root + "124", root + "124a", root + "125", root + "125a", 
		//				  root + "126", root + "126a", root + "127", root + "127a", root + "128", root + "128a",
		//				  root + "129", root + "129a", root + "130", root + "130a", root + "131", root + "131a",
		//				  root + "201", root + "201a", root + "202", root + "202a", root + "203", root + "203a",
		//				  root + "204", root + "204a", root + "205", root + "205a", root + "206", root + "206a", 
		//				  root + "207", root + "207a", root + "208", root + "208a"};
		//		File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));

		//		int cnt=0;
		HashMap<Integer, HashMap<Integer, Double>> blockCoSet = null;
		Jaccard initJMap = null;
		ArrayList<HashMap<Integer, HashMap<Integer, Double>>> corpusCoSetArray = new ArrayList<HashMap<Integer, HashMap<Integer, Double>>>(2);
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

			// build index
			InvertedIndex ii = new InvertedIndex();
			HashMap<Integer, HashSet<Long>> termIndex = ii.buildIndex(stream);			

			// calculate term-term weights
			TermTermWeights ill = new TermTermWeights(termIndex);
			blockCoSet = ill.termCosetBuilder();

			// prints either day by day or one day... check and fix
//						OutTermCosets.printDayByDay(blockCoSet);

			// estimate jaccards array size
			// TODO i think jaccard size can be based on blockcoset size
			int jaccardSize = termIndex.size() / 3;
//			termIndex =null;

			corpusCoSetArray.add(blockCoSet);			// add coset of particular day to array

			if(corpusCoSetArray.size() == 2){
				if(initJMap == null){	// one time initializer
					initJMap = new Jaccard(jaccardSize);
				}
				// do the deed
				Jaccard.getJaccardSimilarity(corpusCoSetArray);
				
//				Jaccard.printResults();
				// swap positions, makes our life easier
				Collections.swap(corpusCoSetArray, 0, 1);
				// remove the first coset array
				corpusCoSetArray.remove(1);
			}
			Thread.sleep(60000);	// give the poor 2GHZ cpu a break
			//			cnt++;
		}
		
		//		OutTermCosets.printDayByDay(corpusCoSetArray);

		// output term trends, with static print method. prints term with list of correlates and weight		
		//		OutTermCosets.printDayByDay(corpusCoSetArray);
		// this is now broken, it is supposed to print out coset from one array.... outdated now since i added support for processing all together
		//		OutTermCosets.printTermCosets(corpusCoSetArray);
	}
}