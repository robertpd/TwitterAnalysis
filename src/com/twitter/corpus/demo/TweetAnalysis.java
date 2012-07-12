package com.twitter.corpus.demo;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
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

import com.twitter.corpus.analysis.CosetSerializer;
import com.twitter.corpus.analysis.InvertedIndex;
import com.twitter.corpus.analysis.Jaccard;
import com.twitter.corpus.analysis.TermTermWeights;
import com.twitter.corpus.data.HtmlStatusCorpusReader;
import com.twitter.corpus.data.StatusStream;
import com.twitter.corpus.types.CoWeight;

public class TweetAnalysis{
	private static final Logger LOG = Logger.getLogger(TweetAnalysis.class);
	public static String output;
	public static String toolsDir;
	private TweetAnalysis() {}
	private static final String INPUT_OPTION = "input";
	private static final String OUTPUT_OPTION = "output";
	private static final String TOOLS = "tools";

	public static void main(String[] args) throws Exception {

		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input directory or file").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("index location").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("stopwords").create(TOOLS));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!(cmdline.hasOption(INPUT_OPTION) && cmdline.hasOption(OUTPUT_OPTION) && cmdline.hasOption(TOOLS))) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(TweetAnalysis.class.getName(), options);
			System.exit(-1);
		}

		int cnt=0;
		output = cmdline.getOptionValue(OUTPUT_OPTION);
		toolsDir = cmdline.getOptionValue(TOOLS);
		String rootBase = cmdline.getOptionValue(INPUT_OPTION);

		String root = rootBase + "/20110";
		//				String root = "/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/html/20110";

		String[] filePaths = {root + "123", root + "123a", root + "124", root + "124a", root + "125", root + "125a", 
				root + "126", root + "126a", root + "127", root + "127a", root + "128", root + "128a",
				root + "129", root + "129a", root + "130", root + "130a", root + "131", root + "131a",
				root + "201", root + "201a", root + "202", root + "202a", root + "203", root + "203a",
				root + "204", root + "204a", root + "205", root + "205a", root + "206", root + "206a", 
				root + "207", root + "207a", root + "208"};

		//		String[] filePaths = {rootBase};
		//		String[] filePaths = {root + "124"};

		//		int cnt=0;
		HashMap<Integer, ArrayList<CoWeight>> blockCoSet = null;
		Jaccard initJMap = null;
		ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corpusCoSetArray = new ArrayList<HashMap<Integer, ArrayList<CoWeight>>>(2);
		for(String path : filePaths){
			LOG.info("Stream number : " + (cnt+1) + "\t. Indexing " + path);
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

			// 1.0 build index
			InvertedIndex ii = new InvertedIndex();
			HashMap<Integer, HashSet<Long>> termIndex = ii.buildIndex(stream);			

			// 2.0 calculate term cosets
			TermTermWeights ill = new TermTermWeights(termIndex);
			blockCoSet = ill.termCosetBuilder();

			// 2.1 serialize term cosets
			CosetSerializer.cosetSerializer(blockCoSet, output, cnt+1);

			corpusCoSetArray.add(blockCoSet);			// add coset of particular day to array
			//
			if(corpusCoSetArray.size() == 2){	// only skipped once at the start
				if(initJMap == null){			// one time initializer
					initJMap = new Jaccard(termIndex.size() + (int)(0.1 * termIndex.size()));	// init size plus 10% for wiggle
				}
				//				// 3.0 do the deed
				int topNTerms = 5;
				Jaccard.getJaccardEnhancedSimilarity(corpusCoSetArray, topNTerms);
				//				Jaccard.getJaccardSimilarity(corpusCoSetArray);
				//				// swap positions, makes our life easier
				Collections.swap(corpusCoSetArray, 0, 1);
				//				// remove the first coset array
				corpusCoSetArray.remove(1);
				//			}
				////						Thread.sleep(30000);
				cnt++;
			}

			// serialize termbimap
			//
			String bimapPath = output + "/termbimap.ser";
			FileOutputStream fileOut = new FileOutputStream(bimapPath);
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
			objectOut.flush();
			objectOut.writeObject(TermTermWeights.termBimap);
			objectOut.close();

			Jaccard.serializeJaccards(output);
		}
	}
}