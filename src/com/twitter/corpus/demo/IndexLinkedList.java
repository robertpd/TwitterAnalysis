package com.twitter.corpus.demo;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.DefaultSimilarity;

import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;

public class IndexLinkedList {	
	public IndexLinkedList(StatusStream stream) throws IOException{
		this.stream = stream;
	}
	public static HashMap<String, Integer> termMap = new HashMap<String, Integer>();
	private StatusStream stream;
	private static final Logger LOG = Logger.getLogger(IndexLinkedList.class);
	
	
	public void Index() throws IOException{
		
		HashMap<String, LinkedList<Integer>> termMatrix = new HashMap<String, LinkedList<Integer>>();
		HashMap<Long, Integer> docMap = new HashMap<Long, Integer>();
		TweetProcessor.callStops();
		int cnt = 0;
		int docCount=0;
		Status status;
			try {
				while ((status = stream.next()) != null)
				{
					String tweet = status.getText();
					if (tweet == null){
						continue;
					}
					// do processing on each tweet
					ProcessedTweet pt = TweetProcessor.processTweet(status.getText(), status.getId());

					for(int i=0; i< pt.termList.size() ; i++){
						if(!termMatrix.containsKey(pt.termList.get(i)))
						{
							// removed docmapping from long to int
//							if(!docMap.containsKey(status.getId()))	{
//								docMap.put(status.getId(), docCount);
//							}
							LinkedList<Integer> termDocTrueIndex = new LinkedList<Integer>();
							termDocTrueIndex.add(docCount);
							termMatrix.put(pt.termList.get(i), termDocTrueIndex);
						}
						else
						{
//							if(!docMap.containsKey(status.getId()))	{
//								docMap.put(status.getId(), docCount);
//							}
							termMatrix.get(pt.termList.get(i)).add(docCount);	// append to linkedlist, will need to be converted to array??? for access speed,, not a sparse array, still flat just taking size of linkedlist as initializer
						}
					}
					docCount++;
					if(docCount == 3000){
						break;
					}
				}			
				//how does number of documents get incorporated
				// also need to move to linkedlists to avoid memory overflow			
				int tCount = termMatrix.size();

				// LL needs a parallel int[] to mark positions within. ie. for quick access
				LinkedList<LinkedList<MtrxCell>> coWeightLL = new LinkedList<LinkedList<MtrxCell>>();

				double[][] coWeight = null;//new double[tCount][tCount];
				Set<String> termKeys = termMatrix.keySet();
				String[] terms = termKeys.toArray(new String[termKeys.size()]);

				//**********************//
				// Co-Weight Matrix
				//**********************//
				for(int i=0 ; i<tCount ; i++){
					LinkedList<MtrxCell> tuple = new LinkedList<MtrxCell>();
					LinkedList<Integer> iList = termMatrix.get(terms[i]);
					for(int j=0 ; j<tCount ; j++){
						double cow = mult(iList, termMatrix.get(terms[j]));
						if( cow > 0.0){
							tuple.add(new MtrxCell(j,cow));
						}
					}
					coWeightLL.add(tuple);
				}
				// compute Matrix of Jaccard's Coefficients
				LinkedList<LinkedList<MtrxCell>> jaccard = new LinkedList<LinkedList<MtrxCell>>();
				// jaccard coeffs for LL version
				ListIterator<LinkedList<MtrxCell>> cowMtrxIter = coWeightLL.listIterator();
				while(cowMtrxIter.hasNext()){
					ListIterator<MtrxCell> cowIter = cowMtrxIter.next().listIterator();
					while(cowIter.hasNext()){
						MtrxCell cow = cowIter.next();
						//					int pos = cow.index
					}
				}
			}

			finally
			{

			}
		LOG.info(String.format("Total of %s statuses indexed", cnt));
	}
	private double mult(LinkedList<Integer> a, LinkedList<Integer> b){
		double retVal = 0.0;
		if(a.size() <= b.size()){
			ListIterator<Integer> li = a.listIterator();
			while(li.hasNext()){
				if(b.contains(li.next())){
					retVal+=1.0;
				}
			}
		}
		if(b.size() < a.size()){
			ListIterator<Integer> li = b.listIterator();
			while(li.hasNext()){
				if(a.contains(li.next())){
					retVal+=1.0;
				}
			}
		}
		return retVal;
	}
	public static class ConstantNormSimilarity extends DefaultSimilarity {
		private static final long serialVersionUID = 2737920231537795826L;

		@Override
		public float computeNorm(String field, FieldInvertState state) {
			return 1.0f;
		}
	}
	// a specific value occuring at an position(index)
	public class MtrxCell{
		public MtrxCell(int i, double val){
			this.index = i;
			this.val = val;
		}
		int index;
		double val;
	}
}
