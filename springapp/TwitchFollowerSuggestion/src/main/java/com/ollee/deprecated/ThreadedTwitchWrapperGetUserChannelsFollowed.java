package com.ollee.deprecated;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.ollee.TwitchWrapper;

import lombok.Getter;
import me.philippheuer.twitch4j.model.Channel;
import me.philippheuer.twitch4j.model.Follow;

public class ThreadedTwitchWrapperGetUserChannelsFollowed implements Runnable {
	@Getter
	private String username;
	private List<Follow> channelFollows = new LinkedList<Follow>();
	private Thread t;
	private Channel channel;
	
	public ThreadedTwitchWrapperGetUserChannelsFollowed(String u){
		username = u;
		channel = TwitchWrapper.getChannelObject(username);
	}
	
	public List<Follow> getUserChannelsFollowedList() {
		// TODO Auto-generated method stub
		return this.channelFollows;
	}

	@Override
	public void run() {
		System.out.println("ThreadedTwitchWrappergetUserchannelsFollows: running TwitchWrapper.getChannelFollwoers(" + username + ")");
		channelFollows.addAll(TwitchWrapper.getChannelFollowers(username));
		System.out.println("ThreadedTwitchWrappergetUserchannelsFollows: got " + channelFollows.size() + "follows for user " + username);
		for(int i = 0; i < channelFollows.size(); i++){
			channelFollows.get(i).setChannel(channel);
		}
		TwitchAPIRateLimiter.addDone(this);
	}
	
	public void start(){
		if(t == null){
			t = new Thread (this, username);
			t.start();
		}
		
	}

}
