package com.ollee.deprecated;

import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import lombok.Getter;
import lombok.Setter;
import me.philippheuer.twitch4j.model.Follow;

public final class TwitchAPIRateLimiter implements Runnable {
	private static Queue<ThreadedGetUserChannelsTimePair> masterUserFollows = new LinkedList<ThreadedGetUserChannelsTimePair>();
	private static Queue<ThreadedTwitchWrapperGetUserChannelsFollowed> finishedUserFollows = new LinkedList<ThreadedTwitchWrapperGetUserChannelsFollowed>();
	private static Thread t;
	private static long lastRun = Date.from(Instant.now()).getTime();
	@Getter
	@Setter
	private static boolean started = false;
	
	public static void addElement(ThreadedTwitchWrapperGetUserChannelsFollowed t){
		masterUserFollows.offer(new ThreadedGetUserChannelsTimePair(t));
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
		return masterUserFollows.size();
	}
	
	public static boolean isEmpty(){
		return masterUserFollows.isEmpty();
	}
	
	@Override
	public void run() {
		setStarted(true);
		while(true){
			if(!masterUserFollows.isEmpty()){
				if(Date.from(Instant.now()).getTime() - lastRun > 1000){
					System.out.println("twitchAPIRateLimiter: Trying to start a fetch thread");
					masterUserFollows.poll().getT().start();
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
		CassandraDriver.threadedInsertFollowList((done.getUserChannelsFollowedList()));
		finishedUserFollows.offer(done);
	}
	
	public static int getFinishedSize(){
		return finishedUserFollows.size();
	}
	
	public static ThreadedTwitchWrapperGetUserChannelsFollowed getFinished(){
		if(finishedUserFollows.size() == 0){
			return null;
		} else{
			return finishedUserFollows.poll();
		}
	}

	public static void enqueueListToFetchFromTwitchAndInsertIntoCassandra(List<Follow> userFollows) {
		Iterator<Follow> iterator = userFollows.iterator();
		Follow follow = null;
		while(iterator.hasNext()){
			follow = iterator.next();
			System.out.println("TwithcAPIRateLimiter: Enqueueing fetch channel followS: " + follow.getChannel().getName().toLowerCase() + " queue size: " + masterUserFollows.size());
			addElement(new ThreadedTwitchWrapperGetUserChannelsFollowed(follow.getChannel().getName().toLowerCase()));
		}
	}
	

}
