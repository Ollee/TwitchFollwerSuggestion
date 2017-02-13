package TwitchApiTest;

import java.util.Iterator;
import java.util.List;

import me.philippheuer.twitch4j.model.Follow;

// client id: 5uuaaq02y9fv6j7phunakg0uoo5xu71

public class TwitchApiTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TwitchWrapper twitch = new TwitchWrapper();
		List<Follow> follows = twitch.getChannelFollowers("leshkee");
		System.out.println("Exited getChannelFollowers with follows.size() " + follows.size());
		Iterator<Follow> followsIterator = follows.iterator();
		System.out.println("Iterator created");
		while(followsIterator.hasNext()){
//			System.out.println("follows iterator loop");
			System.out.println(followsIterator.next().getUser().getDisplayName());
		}
		
//		for(int i=0; i < follows.size(); i++){
//			System.out.println((i+1) + ": " + follows.get(i).getUser().getDisplayName());
//		}
	}
}
