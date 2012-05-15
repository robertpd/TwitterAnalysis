package com.twitter.corpus.demo;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

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

import com.twitter.corpus.data.HtmlStatusBlockReader;
import com.twitter.corpus.data.HtmlStatusCorpusReader;
import com.twitter.corpus.data.JsonStatusBlockReader;
import com.twitter.corpus.data.JsonStatusCorpusReader;
import com.twitter.corpus.data.StatusStream;
import com.twitter.corpus.types.CoWeight;

public class CorpusExtractor2{
	private static final Logger LOG = Logger.getLogger(IndexStatuses.class);
	private CorpusExtractor2() {}

	private static final String INPUT_OPTION = "input";
	private static final String INDEX_OPTION = "index";

	private static final String HTML_MODE = "html";
	private static final String JSON_MODE = "json";

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		System.out.println("Classpath = " + System.getProperty("java.class.path"));

		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input directory or file").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("index location").create(INDEX_OPTION));
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

		TermTermWeights ill = new TermTermWeights(stream);
		HashMap<Integer, ArrayList<CoWeight>> gman = ill.Index();
		
	}
}