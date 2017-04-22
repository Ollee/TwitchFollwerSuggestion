package com.ollee;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ollee.deprecated.TwitchAPIRateLimiter;

import me.philippheuer.twitch4j.model.Channel;
import me.philippheuer.twitch4j.model.Follow;

public final class TwitchAPICallHandler {
	
	private static List<Follow> userFollowsToInsertIntoDatabase = new LinkedList<Follow>();
	private static List<Follow> channelFollowers = new LinkedList<Follow>();
	private static TwitchAPIRateLimiter runMe = new TwitchAPIRateLimiter();
	private static List<String> channelsAlreadyInChannelsDatabase = new LinkedList<String>();
	private static Map<String, Long> channelFollowerCounts = new HashMap<String, Long>();

	public TwitchAPICallHandler(){
	}
	
	public static List<Channel> fetchChannelSuggestions(String username){
		
		channelsAlreadyInChannelsDatabase = CassandraDriver2.getListOfChannelsAlreadyInDatabase();
		
		TwitchAPICallHandler.updateChannelFollowerCounts();
//level1
	//fetch channels user follows
		List<String> userFollows = null;
		
		if(!channelsAlreadyInChannelsDatabase.contains(username)){
			userFollows = TwitchWrapper.getUserChannelsFollowsAsString(username);
			CassandraDriver2.insertFollowList(username, userFollows, true);
		} else{
			userFollows = CassandraDriver2.getFollowList(username);
		}
		
//		userFollows = userFollows.subList(0, 4);//debug reduce size of follows list to make testing faster
//level2
	//fetch followers of each of the channels in userFollows

		//remove channels with more than 10,000 followers
		userFollows.removeAll(TwitchAPICallHandler.getChannelFollowersOverLong(channelFollowerCounts, new Long(10000)));
		//remove followers already in cassandra from a copy of user follows
		List<String> channelsToFetchFollowersOf = removeChannelsAlreadyInDatabase(new LinkedList<String>(userFollows));
		System.out.println("after Remove channels: " + channelsToFetchFollowersOf.size());
		Map<String, List<String>> level2Map = null; 
		//if there are any channels in userFollows, fetch them from twitch and insert to cassandra
		if(!channelsToFetchFollowersOf.isEmpty()){
			//this initiates twithc api calls for things not already fetched
			level2Map =  fetchFollowersOfListOfStringChannels(channelsToFetchFollowersOf);
			//so if this runs, level2Map has queries already run to twitch and inserted into cassandra
		} 
		
		List<String> level2ToFetchFromCassandra = new LinkedList<String>();
		//create list of remaining to fetch from cassandra
		if(level2Map.isEmpty()){
			//if map is empty candidate list = user follows
			level2ToFetchFromCassandra = new LinkedList<String>(userFollows);
		} else{
			//if map not empty, create candidate list to fetch from cassandra
			List<String> copyOfUserFollows = new LinkedList<String>(userFollows);
			copyOfUserFollows.removeAll(level2Map.keySet()); //remove all keys in level2Map from copyOfUserFollows
			level2ToFetchFromCassandra.addAll(copyOfUserFollows);
		}

		level2Map.putAll(CassandraDriver2.getChannelFollowerMap(level2ToFetchFromCassandra));
		
		System.out.println("level2Map dump");
		Iterator<String> tempIter = level2Map.keySet().iterator();
		String key = "";
		while(tempIter.hasNext()){
			key = tempIter.next();
			System.out.println("Key: " + key + " : " + level2Map.get(key));
		}
		//level2Map should now have all followers of the channels I follow...now to test
		
		//now I need to fetch all the channels that users of level2Map have
		//first check for duplicates
		
		
	//this is where fresh code starts

//		System.out.println("userFollows.size: " + userFollows.size());
//		//check my database if the channels in userFollows are cached
//		List<Follow> userFollowsToInsertIntoCassandra = null;
////		if(level1InsertedFlag == true){
//////			userFollowsToInsertIntoCassandra = new LinkedList<Follow>(userFollows);
////		}	else{
//////			userFollowsToInsertIntoCassandra = removeChannelsAlreadyInDatabase(new LinkedList<Follow>(userFollows));
////		}
//		System.out.println("TwitchAPICallHandler: userFollows: " + userFollows.size() + " to be inserted: " + userFollowsToInsertIntoCassandra.size());
//		//enqueue the fetch with TwithAPIRateLimiter.enqueueFetchFromTwitchAndInsertIntoCassandra(channelName)
//			//this fetches the followers of each channel not already in databse
//		System.out.println("userFollowsToInsertIntoCassandra.size: " + userFollowsToInsertIntoCassandra.size());
//		if(!userFollowsToInsertIntoCassandra.isEmpty()){
//			System.out.println("userFollowsToInsertIntoCassandra.isEmpty: false");
//			TwitchAPIRateLimiter.enqueueListToFetchFromTwitchAndInsertIntoCassandra(userFollowsToInsertIntoCassandra.subList(0, 50));
//		}
//		if (!TwitchAPIRateLimiter.isStarted()){
//			runMe.start();
//		}
//		
//level3
	//TODO fetch channels follows by mutual followers of channels user at level1 follows
		
		
		
	//this is where fresh code ends		

		
		return null;
	}

	private static List<String> getChannelFollowersOverLong(Map<String, Long> map, Long cutoff) {
		String key;
		List<String> toRemove = new LinkedList<String>();
		Iterator<String> iter = map.keySet().iterator();
		while(iter.hasNext()){
			key = iter.next();
			if(map.get(key) > cutoff){
				System.out.println("Rmemoving : " + key + " from list");
				toRemove.add(key);
			}
		}
		return toRemove;
	}

	private static void updateChannelFollowerCounts() {
		channelFollowerCounts = CassandraDriver2.getChannelFollowerCounts();
	}

	private static Map<String, List<String>> fetchFollowersOfListOfStringChannels(List<String> channelsToFetchFollowersOf) {
		System.out.println("TwitchAPICallHandler: fetchFollowersOfListOfStringChannels " + channelsToFetchFollowersOf.size());
		Map<String, List<String>> map = new HashMap<String,List<String>>();
		List<String> workingList = null;
		Iterator<String> iter = channelsToFetchFollowersOf.iterator();
		String key = "";
		while(iter.hasNext()){
			System.out.println(key);
			key = iter.next();
			if(TwitchWrapper.getFollowerCount(key) < new Long(10000)){
				workingList = TwitchWrapper.getChannelFollowersAsString(key);
				if(workingList != null){
					map.put(key, workingList);
					CassandraDriver2.insertChannelFollowerList(key, workingList, true);
				}
			} else{
				System.out.println("Skipping on: " + key + " because followerCount > 10000");
			}
		}
		
		
		return map;
	}

	private static List<String> extractChannelNamesAsStringList(List<Follow> followList) {
		System.out.println("TwitchAPICallHandler: extractChannelNameAsString " + followList.size());
		Iterator<Follow> iter = followList.iterator();
		List<String> stringList = new LinkedList<String>();
		while(iter.hasNext()){
			stringList.add(iter.next().getChannel().getName().toLowerCase());
		}
		return stringList;
	}
	
	private static List<String> extractFollowerNamesAsStringList(List<Follow> followList) {
		System.out.println("TwitchAPICallHandler: extractFollowerNamesAsStringList: " + followList.size());
		Iterator<Follow> iter = followList.iterator();
		List<String> stringList = new LinkedList<String>();
		String next = "";
		while(iter.hasNext()){
			next = iter.next().getChannel().getName().toLowerCase();
//			System.out.println("TwitchAPICallHandler: extractFollowerNamesAsStringList: next: " + next);
			stringList.add(next);
		}
		return stringList;
	}

	private static List<String> removeChannelsAlreadyInDatabase(List<String> linkedList) {
		System.out.println("TwitchAPICallHandler: removeChannelsAlreadyInDatabase: " + linkedList.size());
//		List<String> toRemove = new LinkedList<String>();
//		for(String str : linkedList){
//			if(CassandraDriver2.checkIfChannelFollowersAlreadyFetched(str)){
//				toRemove.add(str);
//			}
//		}
		
		linkedList.removeAll(channelsAlreadyInChannelsDatabase);
//		linkedList.removeAll(toRemove);
//		
//		Iterator<String> iterator = linkedList.iterator();
//		while (iterator.hasNext()){
//			String next = iterator.next();
//			if(CassandraDriver2.checkIfChannelFollowersAlreadyFetched(next.toLowerCase())){
//				linkedList.remove(next);
//			}
//		}
//		
		
		return linkedList;
	}

	public static void addToChannelFollowerCounts(String channel, Long followerCount) {
		channelFollowerCounts.put(channel, followerCount);
	}

}

//System.out.println("TwitchAPICallHandler: ");