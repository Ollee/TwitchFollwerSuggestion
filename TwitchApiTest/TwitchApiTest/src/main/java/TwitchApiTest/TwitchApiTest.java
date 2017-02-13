package TwitchApiTest;

import java.util.Iterator;
import java.util.List;

import me.philippheuer.twitch4j.model.Follow;

// client id: 5uuaaq02y9fv6j7phunakg0uoo5xu71

public class TwitchApiTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TwitchWrapper twitch = new TwitchWrapper();
		List<Follow> follows = twitch.getChannelFollowers("ollee64");
		
		Iterator<Follow> followsIterator = follows.iterator();
		while(followsIterator.hasNext()){
			System.out.print(followsIterator.next().getUser().getDisplayName());
		}
	}
}
