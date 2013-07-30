package com.twitter.corpus.analysis;

import java.util.concurrent.TimeUnit;

public class Admin {
	private Admin(){}
	
	/**
	 * Provide a convenience method to calculate formatted duration  
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static String getTime(long startTime, long endTime){
		long diff = endTime - startTime;
		return String.format("%d min, %d sec", 
			    TimeUnit.MILLISECONDS.toMinutes(diff),
			    TimeUnit.MILLISECONDS.toSeconds(diff) - 
			    TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(diff))
			);
	}
}
