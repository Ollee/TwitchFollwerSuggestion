package com.ollee;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.Getter;
import me.philippheuer.twitch4j.TwitchClient;
import me.philippheuer.twitch4j.endpoints.ChannelEndpoint;
import me.philippheuer.twitch4j.endpoints.UserEndpoint;
import me.philippheuer.twitch4j.model.Channel;
import me.philippheuer.twitch4j.model.Follow;
import me.philippheuer.twitch4j.model.User;

public final class TwitchWrapper {
	@Getter
	private static String clientID = "5uuaaq02y9fv6j7phunakg0uoo5xu71";
	@Getter
	private static String clientSecret = "e17btqxk3f85piyf66zvlbmwfi5oc2";
	private static TwitchClient twitchClient = new TwitchClient(TwitchWrapper.getClientID(), TwitchWrapper.getClientSecret());
	
	public TwitchWrapper() {
	}
	public static Channel getChannelObject(String channelName){
		return twitchClient.getChannelEndpoint(channelName).getChannel();
	}

	//get list of of followers a channel has
	public static List<Follow> getChannelFollowers(String channel){
		
		ChannelEndpoint channelEndpoint = twitchClient.getChannelEndpoint(twitchClient.getUserEndpoint().getUserIdByUserName(channel).get());
		Long followerCount = getFollowerCount(channel);
		
		System.out.println("TwitchWrapper: The channel: " + channelEndpoint.getChannel().getName() + " has followerCount: " + followerCount);
		
//		List<Follow> follows = populateListFollowWithChannel(
//								channelEndpoint.getFollowers(Optional.ofNullable(followerCount), Optional.empty()),
//								channelEndpoint.getChannel());
		List<Follow> follows = channelEndpoint.getFollowers(Optional.ofNullable(followerCount), Optional.empty());
//		System.out.println("1234: " + follows.get(0).toString());
		System.out.println("TwitchWrapper: The followers List of: " + channelEndpoint.getChannel().getName() + " has elements n = " + follows.size());

		return follows;
	}

	public static List<String> getChannelFollowersAsString(String channel){
		
		return  TwitchWrapper.getChannelFollowers(channel)
				.stream().map(Follow::getUser).collect(Collectors.toList())
						.stream().map(User::getName).collect(Collectors.toList());
	}
	
	public static Long getFollowerCount(String channel){
		Long followerCount = (long) 0;
		try {
			ChannelEndpoint channelEndpoint = twitchClient.getChannelEndpoint(
													twitchClient.getUserEndpoint().getUserIdByUserName(channel).get());
			followerCount = channelEndpoint.getChannel().getFollowers();
			CassandraDriver3.insertChannelFollowerCount(channel, followerCount);
			TwitchAPICallHandler.addToChannelFollowerCounts(channel, followerCount);
		} catch (Exception e) {
			System.out.println("TiwtchWrapper: getFollowerCount: caught exception");
			e.printStackTrace();
		} 
		return followerCount;
	}
	
	//get list of all channels a user follows
	public static List<Follow> getUserChannelsFollowed(String user){
		UserEndpoint userEndpoint = twitchClient.getUserEndpoint();
		Optional<User> userObject = userEndpoint.getUser(twitchClient.getUserEndpoint().getUserIdByUserName(user).get());
		List<Follow> userFollows = userEndpoint.getUserFollows(	userObject.get().getId(), 
																Optional.ofNullable(new Long(999999999)), 
																Optional.ofNullable(new Long(0)), 
																Optional.empty(), 
																Optional.empty());
//		System.out.println("TwitchWrapper: " + user + " follows: " + userFollows.size() + " channels");
		return userFollows;
	}
	
	public static List<String> getUserChannelsFollowsAsString(String user){
		
		return TwitchWrapper.getUserChannelsFollowed(user)
				.stream().map(Follow::getChannel).collect(Collectors.toList())
					.stream().map(Channel::getName).collect(Collectors.toList());
	}
	
	private static List<Follow> populateListFollowWithChannel(List<Follow> follows, Channel channel){
		Iterator<Follow> iter = follows.iterator();
		List<Follow> workingList = new LinkedList<Follow>();
		Follow workingFollow = new Follow();
		while(iter.hasNext()){
			workingFollow = iter.next();
			workingFollow.setChannel(channel);
			workingList.add(workingFollow);
		}
		return workingList;
	}
	public static boolean channelExists(String username){
		try {
			twitchClient.getChannelEndpoint(username);
		} catch (Exception e) {
			return false;
		}
		
		return true;
	}


	
}
