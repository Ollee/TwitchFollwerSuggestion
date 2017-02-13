package TwitchApiTest;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import me.philippheuer.twitch4j.*;
import me.philippheuer.twitch4j.endpoints.ChannelEndpoint;
import me.philippheuer.twitch4j.endpoints.UserEndpoint;
import me.philippheuer.twitch4j.model.Channel;
import me.philippheuer.twitch4j.model.Follow;
import me.philippheuer.twitch4j.model.User;

public class TwitchWrapper {
	private String clientID = "5uuaaq02y9fv6j7phunakg0uoo5xu71";
	private String clientSecret = "e17btqxk3f85piyf66zvlbmwfi5oc2";
	private TwitchClient twitchClient;
	
	public TwitchWrapper() {
		twitchClient = new TwitchClient(this.getClientID(), this.getClientSecret());
	}

	private void build(){
		TwitchClient twitch = new TwitchClient(this.getClientID(), this.getClientSecret());
		ChannelEndpoint channelEndpoint;
		int followerCount;
		Optional<Long> channelId = twitch.getUserEndpoint().getUserIdByUserName("ollee64");
		
		
		channelEndpoint = twitch.getChannelEndpoint(channelId.get());
		followerCount = channelEndpoint.getChannel().getFollowers();
		
		System.out.println("follower count of channel: " + channelEndpoint.getChannel().getDisplayName() + " has followerCount: " + followerCount);
		
		List<Follow> follows = channelEndpoint.getFollowers(Optional.ofNullable(followerCount), Optional.empty());
		System.out.println("followers list contans items n= " + follows.size());
		
		Iterator<Follow> followsIterator = follows.iterator();
		while(followsIterator.hasNext()){
			System.out.print(followsIterator.next().getUser().getDisplayName());
		}
	}

	public String getClientID() {
		return this.clientID;
	}
	private String getClientSecret() {
		return this.clientSecret;
	}
	public List<Follow> getChannelFollowers(String channel){
		
		Optional<Long> channelId = twitchClient.getUserEndpoint().getUserIdByUserName(channel);
		ChannelEndpoint channelEndpoint = twitchClient.getChannelEndpoint(channelId.get());
		int followerCount = channelEndpoint.getChannel().getFollowers();
		
		System.out.println("The channel: " + channelEndpoint.getChannel().getDisplayName() + " has followerCount: " + followerCount);
		
		List<Follow> follows = channelEndpoint.getFollowers(Optional.ofNullable(followerCount), Optional.empty());
		
		System.out.println("The followers List of: " + channelEndpoint.getChannel().getDisplayName() + " has elements n = " + follows.size());
		
		return follows;
	}
	public List<Channel> getUserChannelsFollowed(String user){
		Optional<Long> userId = twitchClient.getUserEndpoint().getUserIdByUserName(user);
		//TODO build getUserChannelsFollows
		//get user endpoint
		// get user object
		//get channels followed
		//return list of channels followed
		return null;
	}

	
}
