package com.twitter.corpus.data;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.io.pair.PairOfLongString;

/**
 * Abstraction for a stream of statuses, backed by an underlying SequenceFile.
 */
public class HtmlStatusBlockReader implements StatusStream {
	private SequenceFile.Reader reader;

	private final PairOfLongString key = new PairOfLongString();
	private final HtmlStatus value = new HtmlStatus();

	public HtmlStatusBlockReader(Path path, FileSystem fs) /*throws IOException*/ {
		Preconditions.checkNotNull(path);
		Preconditions.checkNotNull(fs);
		try
		{
			reader = new SequenceFile.Reader(fs, path, fs.getConf());    
		}
		catch (IOException exc){
//			reader = null;
		}
	}

	/**
	 * Returns the next status, or <code>null</code> if no more statuses.
	 */
	public Status next() throws IOException {
		if (!reader.next(key, value))
			return null;

		return Status.fromHtml(key.getLeftElement(), key.getRightElement(),
				value.getHttpStatusCode(), value.getHtml());
	}

	public void close() throws IOException {
		reader.close();
	}
}
