package com.twitter.corpus.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.twitter.corpus.data.HtmlStatusBlockReader;
import com.twitter.corpus.data.HtmlStatusCorpusReader;
import com.twitter.corpus.data.JsonStatusBlockReader;
import com.twitter.corpus.data.JsonStatusCorpusReader;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;
import com.twitter.corpus.demo.IndexStatuses.ConstantNormSimilarity;
import com.twitter.corpus.demo.IndexStatuses.StatusField;

public class IndexHashes  {
	private static final Logger LOG = Logger.getLogger(IndexStatuses.class);

	private IndexHashes() {}
//	public static Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_31);

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

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
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

		String lepath = "/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/stops.txt";
		BufferedReader reader = new BufferedReader(new FileReader(lepath));
		Set<String> stopwords = new HashSet<String>();
		while (reader.readLine() != null){
			stopwords.add(reader.readLine().trim());
		}
		Analyzer analyzer = new TweetAnalyzer(Version.LUCENE_31, stopwords);
		
//		Analyzer analyzer = ANALYZER;
		Similarity similarity = new ConstantNormSimilarity();
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_31, analyzer);
		config.setSimilarity(similarity);
		config.setOpenMode(IndexWriterConfig.OpenMode.CREATE); // Overwrite existing.

		IndexWriter writer = new IndexWriter(FSDirectory.open(indexLocation), config);
		Set<String> stops = TermDocIDs.readFile(new String[]{"/home/dock/Documents/IR/DataSets/stopwords/english"});
		
		int cnt = 0;
		Status status;
		TermDocIDs.callStopwords();
		try 
		{
//			int c=0;
			while ((status = stream.next()) != null) 
			{
				if (status.getText() == null) 
				{
					continue;
				}
				String hashes = getElements(status.getText(), hash);
				String mentions = getElements(status.getText(), mention);
				cnt++;

				Document doc = new Document();

				if(hashes.length() != 0){
					doc.add(new Field(HashField.HASH.name, hashes, Store.YES, Index.ANALYZED));
				}
				if(mentions.length() != 0){
					doc.add(new Field(HashField.MENTION.name, mentions, Store.YES, Index.ANALYZED));
				}

				String[] prep = TermDocIDs.preprocessTweet(status.getText());
				String[] tweetStopped = TermDocIDs.removeStops(prep);
				
				doc.add(new Field(HashField.TEXT.name, rebuildTw(tweetStopped)/*status.getText()*/, Store.YES, Index.ANALYZED, TermVector.YES));

				for(int i=0; i<tweetStopped.length ;i++){
					if(tweetStopped[i]!=null &&  i!=tweetStopped.length-1){
						if(tweetStopped[i+1]!=null){
							doc.add(new Field("bigram", tweetStopped[i] +" "+ tweetStopped[i+1],Store.YES,Index.NOT_ANALYZED));
						}
					}
				}
				
				doc.add(new Field(HashField.ID.name, status.getId() + "", /* String.valueOf(status.getId()) */ Store.YES, Index.ANALYZED/*Index.NOT_ANALYZED_NO_NORMS*/));
				doc.add(new Field(HashField.SCREEN_NAME.name, status.getScreenname(),  Store.YES, Index.NOT_ANALYZED_NO_NORMS));
				doc.add(new Field(HashField.CREATED_AT.name, status.getCreatedAt(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));

				String createdAt[] = status.getCreatedAt().split(" ");
				String createDay = new StringBuffer().append(createdAt[1]).append("_").append(createdAt[2]).toString();
				doc.add(new Field(StatusField.DAY.name, createDay, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
				writer.addDocument(doc);

				if (cnt % 10000 == 0) 
				{
					LOG.info(cnt + " statuses indexed");
				}
//				if(cnt % 100000 == 0){
//					Thread.currentThread().sleep(10000);
//				}
//				c++;
//				if(c==10000){
//					break;
//				}
			}
			LOG.info("Optimizing index...");
//			Thread.currentThread().sleep(100000);
			writer.optimize();
			writer.close();
		} finally 
		{
			stream.close();
		}
		LOG.info(String.format("Total of %s statuses indexed", cnt));

	}

	private static String rebuildTw(String[] tweet){
		StringBuilder retVal = new StringBuilder();
		for(String s: tweet){
			retVal.append(s + " ");
		}
		return retVal.toString().trim();
	}
	
	private static String getElements(String s, String t){
		StringBuilder buffer = new StringBuilder();
		String[] elements = s.split(" ");

		for(int i=0; i < elements.length; i++){
			if(elements[i].startsWith(t, 0)){
				buffer.append(elements[i]);
				buffer.append(" ");
			}
		}
		return buffer.toString().trim(); 
	}

	public static class ConstantNormSimilarity extends DefaultSimilarity {
		private static final long serialVersionUID = 2737920231537795826L;

		@Override
		public float computeNorm(String field, FieldInvertState state) {
			return 1.0f;
		}
	}

}
