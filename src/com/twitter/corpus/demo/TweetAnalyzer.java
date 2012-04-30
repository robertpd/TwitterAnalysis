package com.twitter.corpus.demo;

import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

//public class TweetAnalyzer extends ReusableAnalyzerBase {
public class TweetAnalyzer extends StopwordAnalyzerBase {
	private Version matchVersion;
	/**
	 * Creates a new {@link TweetAnalyzer}.
	 * @param stopwords 
	 */
	public TweetAnalyzer(Version matchVersion, Set<String> stopwords) {
		super(matchVersion, stopwords);
		this.matchVersion = matchVersion;
	}

	@Override
	protected  TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
		return new TokenStreamComponents(new LowerCaseHashtagMentionPreservingTokenizer(matchVersion, reader));
	}
}