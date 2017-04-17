package com.ollee;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import javassist.bytecode.Descriptor.Iterator;
import me.philippheuer.twitch4j.TwitchClient;
import me.philippheuer.twitch4j.endpoints.ChannelEndpoint;
import me.philippheuer.twitch4j.endpoints.UserEndpoint;
import me.philippheuer.twitch4j.model.Channel;
import me.philippheuer.twitch4j.model.Follow;
import me.philippheuer.twitch4j.model.User;

public final class TwitchWrapper {
	private static String clientID = "5uuaaq02y9fv6j7phunakg0uoo5xu71";
	private static String clientSecret = "e17btqxk3f85piyf66zvlbmwfi5oc2";
	private static TwitchClient twitchClient;
	
	public TwitchWrapper() {
		try{
			twitchClient = new TwitchClient(TwitchWrapper.getClientID(), TwitchWrapper.getClientSecret());
			}
		catch (Exception e){
			System.out.println("TwitchWrapper: Exception caught from TwitchClient: " + e.toString());
			e.printStackTrace();
			}
	}
	public static Channel getChannelObject(String channelName){
		return twitchClient.getChannelEndpoint(channelName).getChannel();
	}

	//get list of of followers a channel has
	public static List<Follow> getChannelFollowers(String channel){
		
		Optional<Long> channelId = twitchClient.getUserEndpoint().getUserIdByUserName(channel);
		ChannelEndpoint channelEndpoint = twitchClient.getChannelEndpoint(channelId.get());
		Long followerCount = channelEndpoint.getChannel().getFollowers();
		
		System.out.println("The channel: " + channelEndpoint.getChannel().getDisplayName() + " has followerCount: " + followerCount);
		
		List<Follow> follows = channelEndpoint.getFollowers(Optional.ofNullable(followerCount), Optional.empty());
		
		System.out.println("TwitchWrapper: The followers List of: " + channelEndpoint.getChannel().getDisplayName() + " has elements n = " + follows.size());

		return follows;
	}
	
	private static String getClientID() {
		return TwitchWrapper.clientID;
	}
	
	private static String getClientSecret() {
		return TwitchWrapper.clientSecret;
	}
	
	//get list of all channels a user follows
	public static List<Follow> getUserChannelsFollowed(String user){
		//set client to user
		Optional<Long> userId = twitchClient.getUserEndpoint().getUserIdByUserName(user);
		//get user endpoint
		UserEndpoint userEndpoint = twitchClient.getUserEndpoint();
		// get user object
		Optional<User> userObject = userEndpoint.getUser(userId.get());
		//get channels follows
		List<Follow> userFollows = userEndpoint.getUserFollows(userObject.get().getId(), Optional.ofNullable(new Long(999999999)), Optional.ofNullable(new Long(0)), Optional.empty(), Optional.empty());
		System.out.println("TwitchWrapper: " + user + " follows: " + userFollows.size() + " channels");
		//return list of channels followed
		return userFollows;
	}

	
}

//old code
//	private void build(){
//		TwitchClient twitch = new TwitchClient(this.getClientID(), this.getClientSecret());
//		ChannelEndpoint channelEndpoint;
//		int followerCount;
//		Optional<Long> channelId = twitch.getUserEndpoint().getUserIdByUserName("ollee64");
//		
//		
//		channelEndpoint = twitch.getChannelEndpoint(channelId.get());
//		followerCount = channelEndpoint.getChannel().getFollowers();
//		
//		System.out.println("follower count of channel: " + channelEndpoint.getChannel().getDisplayName() + " has followerCount: " + followerCount);
//		
//		List<Follow> follows = channelEndpoint.getFollowers(Optional.ofNullable(followerCount), Optional.empty());
//		System.out.println("followers list contans items n= " + follows.size());
//		
//		Iterator<Follow> followsIterator = follows.iterator();
//		while(followsIterator.hasNext()){
//			System.out.print(followsIterator.next().getUser().getDisplayName());
//		}
//	}