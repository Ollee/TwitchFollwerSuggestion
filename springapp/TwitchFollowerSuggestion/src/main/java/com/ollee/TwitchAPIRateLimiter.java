package com.ollee;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;

public final class TwitchAPIRateLimiter implements Runnable {
	private static Queue<ThreadedGetUserChannelsTimePair> master = new LinkedList<ThreadedGetUserChannelsTimePair>();
	private static Queue<ThreadedTwitchWrapperGetUserChannelsFollowed> finished = new LinkedList<ThreadedTwitchWrapperGetUserChannelsFollowed>();
	private static Thread t;
	private static long lastRun = Date.from(Instant.now()).getTime();
	
	public static void addElement(ThreadedTwitchWrapperGetUserChannelsFollowed t){
		master.offer(new ThreadedGetUserChannelsTimePair(t));
	}
	
	public TwitchAPIRateLimiter(){
	}
	
//	public static boolean canIRun(ThreadedTwitchWrapperGetUserChannelsFollowed thread){
//		if(master.size() != 0){
//			if((Date.from(Instant.now()).getTime() - master.peek().getDate()) > 1000){
//				return true;
//			} else{
//				return false;
//			}
//		}
//		return false;
//	}
	
	public static int getNumberOfThreads(){
		return master.size();
	}
	
	public static boolean isEmpty(){
		return master.isEmpty();
	}
	
	@Override
	public void run() {
		while(true){
			if(!master.isEmpty()){
				if(Date.from(Instant.now()).getTime() - lastRun > 1000){
					System.out.println("twitchAPIRateLimiter: Trying to start a fetch thread");
					master.poll().getT().start();
					lastRun = Date.from(Instant.now()).getTime();
				}
			}
		}
	}
	
	public void start(){
		if(t == null){
			t = new Thread (this, "queue");
			t.start();
		}
		
	}

	public static void addDone(ThreadedTwitchWrapperGetUserChannelsFollowed done) {
		System.out.println("TwitchAPIRateLimiter: finished thread added: " + done.getUsername() + " with channels size: " + done.getUserChannelsFollowedList().size());
		finished.offer(done);
	}
	
	public static int getFinishedSize(){
		return finished.size();
	}
	public static ThreadedTwitchWrapperGetUserChannelsFollowed getFinished(){
		if(finished.size() == 0){
			return null;
		} else{
			return finished.poll();
		}
	}
}
