package com.ollee;

import java.util.LinkedList;
import java.util.List;

import me.philippheuer.twitch4j.model.Follow;

public class ThreadedTwitchWrapperGetUserChannelsFollowed implements Runnable {
	private String username;
	private List<Follow> channelFollows = new LinkedList<Follow>();
	private Thread t;
	
	public ThreadedTwitchWrapperGetUserChannelsFollowed(String u){
		username = u;
	}
	
	public List<Follow> getUserChannelsFollowedList() {
		// TODO Auto-generated method stub
		return this.channelFollows;
	}

	@Override
	public void run() {
		while(!TwitchAPIRateLimiter.canIRun(this)){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Hung up in a thread to get user channels followed");
		}
		System.out.println("running TwitchWrapper.getChannelFollwoers(" + username + ")");
		channelFollows.addAll(TwitchWrapper.getChannelFollowers(username));
	}
	
	public void start(){
		if(t == null){
			t = new Thread (this, username);
			t.start();
		}
		
	}

}
