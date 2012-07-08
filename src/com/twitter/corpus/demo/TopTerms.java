package com.twitter.corpus.demo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Hashtable;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class TopTerms {

	public static int defaultNumTerms = 200;

	public static void main(String[] args) throws Exception {
		Directory dir = FSDirectory.open(new File(args[0]));
		String[] topTerms = {"mention"};
		TermInfo[] terms = getHighFreqTerms(IndexReader.open(dir), null, Integer.parseInt(args[1]), topTerms);
		BufferedWriter out = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/top_" +args[1] + "_" + topTerms[0].toString() + ".txt"));
		for (int i = 0; i < terms.length; i++) {
			System.out.println(i + ".\t" + terms[i].term.text());
//			System.out.println(terms[i].term.text());
//			StringBuffer buf = new StringBuffer();
			
//			out.write(buf.append(i).append(".\t").append(terms[i].term.text()).toString());
			out.write(i);
		}
	}

	public static TermInfo[] getHighFreqTerms(IndexReader reader, Hashtable junkWords, int numTerms, String[] fields) throws Exception {
		if (reader == null || fields == null) return null;
		TermInfoQueue tiq = new TermInfoQueue(numTerms);
		TermEnum terms = reader.terms();
		int c=0;
		int j=0;
		int minFreq = 0;
		while (terms.next()) {
			String field = terms.term().field();
			if (fields != null && fields.length > 0) {
				boolean skip = true;
				for (int i = 0; i < fields.length; i++) {
					if (field.equals(fields[i])) {
						skip = false;
						break;
					}
				}
				c++;
				if (skip) continue;
			}
//			if (junkWords != null && junkWords.get(terms.term().text()) != null) continue;
			
			if (terms.docFreq() > minFreq) {
				tiq.insertWithOverflow(new TermInfo(terms.term(), terms.docFreq()));
				if (tiq.size() >= numTerms) 		     // if tiq overfull
				{
					tiq.pop();				     // remove lowest in tiq
					minFreq = ((TermInfo)tiq.top()).docFreq; // reset minFreq
				}
				j++;
			}
		}
		TermInfo[] res = new TermInfo[tiq.size()];
		for (int i = 0; i < res.length; i++) {
			res[res.length - i - 1] = (TermInfo)tiq.pop();
		}
		return res;
	}
}


