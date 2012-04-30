package com.twitter.corpus.demo;

import java.util.concurrent.TimeUnit;

public class Admin {
	private Admin(){}
	
	public static String getTime(long startTime, long endTime){
		long diff = endTime - startTime;
		return String.format("%d min, %d sec", 
			    TimeUnit.MILLISECONDS.toMinutes(diff),
			    TimeUnit.MILLISECONDS.toSeconds(diff) - 
			    TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(diff))
			);
	}
}
