package com.twitter.corpus.demo;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

//doesnt work due to unexpected type read erfror

public class VectorRead {

	public static void main(String[] args) throws IOException {

	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	SequenceFile.Reader read = new SequenceFile.Reader(fs, new Path("/home//dock//Documents//IR//DataSets//lintool-twitter-corpus-tools-d604184//ids//lv//out.txt"), conf);
	IntWritable dicKey = new IntWritable();
	Text text = new Text();
	Map dictionaryMap = new HashMap();
	int c=0;
	
	while (read.next(text, dicKey) != false) {
		if(c>0){
			dictionaryMap.put(Integer.parseInt(dicKey.toString()), text.toString());
		}
		c++;
	}
	
	read.close();

	//	Configuration conf = new Configuration();
	//	FileSystem fs = FileSystem.get(conf);
	String vectorsPath = "//home//dock//Documents//IR//DataSets//lintool-twitter-corpus-tools-d604184//ids";
	Path path = new Path(vectorsPath);

	SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
	LongWritable key = new LongWritable();
	VectorWritable value = new VectorWritable();

	while (reader.next(key, value)) {
		NamedVector namedVector = (NamedVector)value.get();
		RandomAccessSparseVector vect = (RandomAccessSparseVector)namedVector.getDelegate();

		for( org.apache.mahout.math.Vector.Element e : vect ){
			System.out.println("Token: "+dictionaryMap.get(e.index())+", TF-IDF weight: "+e.get()) ;
		}
	}
	reader.close();
}
}