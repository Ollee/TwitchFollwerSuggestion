package TwitchApiTest;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import me.philippheuer.twitch4j.*;
import me.philippheuer.twitch4j.endpoints.ChannelEndpoint;
import me.philippheuer.twitch4j.model.User;

public class TwitchWrapper {
	private String ClientID;

	public TwitchWrapper(String c) {
		this.setClientID(c);
		this.build();
	}

	private void build(){
		TwitchClient twitch = new TwitchClient(this.getClientID(), "e17btqxk3f85piyf66zvlbmwfi5oc2");
		ChannelEndpoint channelEndpoint;
		Optional<Long> channelId = twitch.getUserEndpoint().getUserIdByUserName("flamegoat");
		
		if(channelId.isPresent()){
			channelEndpoint = twitch.getChannelEndpoint(channelId.get());
		}else{
			System.out.println("Error getting userID");
		}
		
		channelEndpoint.getFollowers(arg0, arg1, arg2)
		
	}
	public void setClientID(String clientID) {
		ClientID = clientID;
	}

	public String getClientID() {
		return ClientID;
	}
	
}
