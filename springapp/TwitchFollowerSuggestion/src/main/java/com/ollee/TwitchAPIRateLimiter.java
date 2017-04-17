package com.ollee;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;

public final class TwitchAPIRateLimiter {
	static Queue<ThreadedGetUserChannelsTimePair> master = new LinkedList<ThreadedGetUserChannelsTimePair>();
	
	public static void addElement(ThreadedTwitchWrapperGetUserChannelsFollowed t){
		master.offer(new ThreadedGetUserChannelsTimePair(t));
	}
	
	public static boolean canIRun(ThreadedTwitchWrapperGetUserChannelsFollowed thread){
		if(master.size() != 0){
			if((Date.from(Instant.now()).getTime() - master.peek().getDate()) > 1000){
				return true;
			} else{
				return false;
			}
		}
		return false;
	}
	
	public static int getNumberOfThreads(){
		return master.size();
	}
	
	public static boolean isEmpty(){
		return master.isEmpty();
	}
	
}
