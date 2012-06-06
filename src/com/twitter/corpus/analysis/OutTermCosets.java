package com.twitter.corpus.analysis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.twitter.corpus.types.CoWeight;

public class OutTermCosets {
	public static void print(ArrayList<HashMap<Integer, ArrayList<CoWeight>>> corpusCoSetArray){
		try{
			if((corpusCoSetArray != null )&& (TermTermWeights.termBimap != null)){
				Set<Integer> allKeys = new HashSet<Integer>();
				for(int day = 0; day < corpusCoSetArray.size()-1; day++){
					allKeys.addAll(corpusCoSetArray.get(day).keySet());						
				}
				BufferedWriter out = new BufferedWriter(new FileWriter("/home/dock/Documents/IR/DataSets/lintool-twitter-corpus-tools-d604184/tweetIndex/all.txt"));

				Iterator<Integer> termKeysIter = allKeys.iterator();
				while(termKeysIter.hasNext()){
					Integer term = termKeysIter.next();
					int skip=0;
					for(int i=0; i < corpusCoSetArray.size(); i++){
						ArrayList<CoWeight> coWeightArrayList = corpusCoSetArray.get(i).get(term);
						if(coWeightArrayList == null)
							skip ++;
					}
					if(skip>10){
						continue;
					}
					for(int i=0; i < corpusCoSetArray.size(); i++){
						ArrayList<CoWeight> coWeightArrayList = corpusCoSetArray.get(i).get(term);

						// want to block all print outs unless theres is data for 15 out of 17 days
						StringBuffer sb = new StringBuffer();
						if(coWeightArrayList==null){
							out.append("Term: "+TermTermWeights.termBimap.inverse().get(term)+", Day: "+(i+1)+": { NULL }\n");
						}
						else{
							boolean newLine = false;
							boolean beginLine = true;
							short printLimit = 0;
							for(CoWeight cw : coWeightArrayList){
								if(cw == null){
									sb.append("Term: "+TermTermWeights.termBimap.inverse().get(term)+", Day: "+(i+1)+" { NULL }\n");
								}
								else{
									if(beginLine){
										sb.append("Term: "+TermTermWeights.termBimap.inverse().get(term)+", Day : "+(i+1)+" { ");
										beginLine = false;
									}
									//									if(cw.correlate > 0.2){
									if(printLimit < 5){
										newLine = true;
										sb.append(TermTermWeights.termBimap.inverse().get(cw.termId)+ ": " + cw.correlate + ", ");
										printLimit++;
									}
									//									}
								}
							}
							if(newLine){
								out.write(sb.replace(sb.length()-2, sb.length()-2,"").append(" }\n").toString());
							}
						}
					}
				}
				out.close();
			}
			System.out.print("finito");
		}catch(Exception ex){
			System.out.print("asd");
		}
	}
}
