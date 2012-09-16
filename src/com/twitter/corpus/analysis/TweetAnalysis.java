package com.twitter.corpus.analysis;

import java.util.ArrayList;
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
import org.mortbay.log.Log;

import com.twitter.corpus.data.HtmlStatusCorpusReader;
import com.twitter.corpus.data.StatusStream;
import com.twitter.corpus.types.CoWeight;
import com.twitter.corpus.types.Serialization2;

public class TweetAnalysis{
	private static final Logger LOG = Logger.getLogger(TweetAnalysis.class);
	public static String output;
	public static String toolsDir;
	private TweetAnalysis() {}
	private static final String INPUT_OPTION = "input";
	private static final String OUTPUT_OPTION = "output";
	private static final String TOOLS = "tools";
	private static final String LOWER_DAILY_THRESH = "gt";
	private static final String UPPER_DAILY_THRESH = "lt";
	private static final String TERM_CORRELATION_THRESH = "m";
	private static final String COSET_TERMS = "t";

	public static int lowCutoffGlobal = 2;
	public static HashMap<Integer, HashSet<Long>> corpusIndex;
	private static ArrayList<HashMap<Integer, HashSet<Long>>> intervalIndices;
	private static int indexDocCount=0;

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input directory or file").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("index location").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("stopwords").create(TOOLS));
		options.addOption(OptionBuilder.withArgName("constant").hasArg().withDescription("lower daily term threshold").create(LOWER_DAILY_THRESH));
		options.addOption(OptionBuilder.withArgName("constant").hasArg().withDescription("upper daily term threshold").create(UPPER_DAILY_THRESH));
		options.addOption(OptionBuilder.withArgName("constant").hasArg().withDescription("term correlation minimum").create(TERM_CORRELATION_THRESH));
		options.addOption(OptionBuilder.withArgName("constant").hasArg().withDescription("coset top terms").create(COSET_TERMS));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try { cmdline = parser.parse(options, args);} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage()); System.exit(-1);	}

		if (!(cmdline.hasOption(INPUT_OPTION) && cmdline.hasOption(OUTPUT_OPTION) && cmdline.hasOption(TOOLS))) {
			HelpFormatter formatter = new HelpFormatter(); 	formatter.printHelp(TweetAnalysis.class.getName(), options); System.exit(-1); }

		int cnt=0;
		int termCosetCounter=0;

		// command line options
		output = cmdline.getOptionValue(OUTPUT_OPTION);
		toolsDir = cmdline.getOptionValue(TOOLS);
		String rootBase = cmdline.getOptionValue(INPUT_OPTION);
		int lowerFreq = Integer.parseInt(cmdline.getOptionValue(LOWER_DAILY_THRESH));
		int upperFreq = Integer.parseInt(cmdline.getOptionValue(UPPER_DAILY_THRESH));
		double m = Double.parseDouble(cmdline.getOptionValue(TERM_CORRELATION_THRESH));
		int cosetTopN = Integer.parseInt(cmdline.getOptionValue(COSET_TERMS));

		
		
		String root = rootBase + "/20110";
		String[] filePaths = {root + "123", root + "123a", root + "124", root + "124a", root + "125", root + "125a", root + "126", root + "126a",
				root + "127", root + "127a", root + "128", root + "128a", root + "129", root + "129a", root + "130", root + "130a", root + "131", 
				root + "131a", root + "201", root + "201a", root + "202", root + "202a", root + "203", root + "203a", root + "204", root + "204a", 
				root + "205", root + "205a", root + "206", root + "206a", root + "207", root + "207a", root + "208"};

		corpusIndex = new HashMap<Integer, HashSet<Long>>(10000);

		HashMap<Integer, ArrayList<CoWeight>> blockCoSet = null;
		Jaccard jaccardSim = null;
		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corpusCoSetArray = new ArrayList<HashMap<Integer, ArrayList<CoWeight>>>(33);
		//		HashMap<Integer, HashSet<Long>> intervalTermIndex = null;

		int corpSize=0;int docCount=0;
		intervalIndices = new ArrayList<HashMap<Integer,HashSet<Long>>>(33);		

		long preIndexingTime = System.currentTimeMillis();
		// Do the indexing
		for(String path : filePaths){
			LOG.info("Stream number : " + (cnt+1) + "\t. Indexing " + path);
			StatusStream stream = null;	FileSystem fs = FileSystem.get(new Configuration());Path file = new Path(path);
			if (!fs.exists(file)) {	System.err.println("Error: " + file + " does not exist!"); System.exit(-1);}
			if (fs.getFileStatus(file).isDir()) {stream = new HtmlStatusCorpusReader(file, fs);	}

			// 1.0 build index
			InvertedIndex ii = new InvertedIndex();
			HashMap<Integer, HashSet<Long>> intervalTermIndex = ii.buildIndex(stream);
			corpSize = corpusIndex.size();

			//add interval index (both old and new terms with their document sets) 
			InvertedIndex.mergeLocalIndex(intervalTermIndex);
			intervalIndices.add(intervalTermIndex);

			docCount = InvertedIndex.getDocCount(corpusIndex);
			cnt++;
		}
		long postIndexingTime = System.currentTimeMillis();
		
		LOG.info("Global corpus index terms: " + corpusIndex.size() + " " + ( corpusIndex.size() - corpSize) + " terms added. " + (docCount - indexDocCount) + " term-document occurences.");

		// trim all local indexes
		ArrayList<HashMap<Integer, HashSet<Long>>> trimmedLocalIndexArray = InvertedIndex.trimLocalIndices(intervalIndices, lowerFreq, upperFreq);		
		long preCosetTime = System.currentTimeMillis();
		long totalCoset = 0;
		// get Term Coset
		for(int i = 0; i < trimmedLocalIndexArray.size(); i++){
			TermTermWeights ill = new TermTermWeights(trimmedLocalIndexArray.get(i));
			long beforeCoset = System.currentTimeMillis();
			blockCoSet = ill.termCosetBuilder(m);
			long afterCoset = System.currentTimeMillis();
			totalCoset += (afterCoset - beforeCoset);
			
			CosetSerializer.cosetSerializer(blockCoSet, output, (termCosetCounter + 1));
			corpusCoSetArray.add(blockCoSet);			// add coset of particular day to array
			termCosetCounter++;
		}		
		CosetSerializer.copusCosetSer(corpusCoSetArray, output);
		
		long beforeJaccard = System.currentTimeMillis();
		// Get Jaccard based on the TC calculated above using m = Arg
		jaccardSim = null;
		jaccardSim = new Jaccard(trimmedLocalIndexArray.get(0).size());

		for(int i = 0 ; i < corpusCoSetArray.size()-1; i++){
			ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corp = new ArrayList<HashMap<Integer,ArrayList<CoWeight>>>(2);

			corp.add(corpusCoSetArray.get(i));
			corp.add(corpusCoSetArray.get(i+1));

			jaccardSim.getJaccardSimilarity(corp, cosetTopN);
			jaccardSim.getJaccardWeightedSimilarity(corp, cosetTopN);
		}
		long afterJaccard = System.currentTimeMillis();
		Jaccard.serializeJaccards(output);

		// print frequency range
		InvertedIndex.printFrequencies(TweetAnalysis.corpusIndex, output + "freqs.txt");
		Serialization2.serialize(TermTermWeights.termBimap, output + "/termbimap.ser");
//		TermTermWeights.serializeTermBimap(output + "/termbimap.ser");
		Serialization2.serialize(corpusIndex, output + "globalIndex.ser");
		Serialization2.serialize(intervalIndices, output + "LocalIndexArray.ser");
//		InvertedIndex.globalIndexSerialize(corpusIndex, output);
//		InvertedIndex.localIndexArraySerialize(intervalIndices, output);
		
		LOG.info("Total time: " + Admin.getTime(startTime, afterJaccard));
		Log.info("Indexing time: " + Admin.getTime(preIndexingTime, postIndexingTime));
		Log.info("Total clustering time: " + Admin.getTime(preCosetTime, preCosetTime + totalCoset));
	}
}