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
	private static Long followerCountCutoff = new Long(3000);

	public TwitchAPICallHandler(){
	}
	
	public static List<Channel> fetchChannelSuggestions(String username){
		
		channelsAlreadyInChannelsDatabase = CassandraDriver2.getListOfChannelsAlreadyInDatabase();
		
		TwitchAPICallHandler.updateChannelFollowerCounts();
		
//level1
	//fetch channels user follows
		List<String> userFollows = fetchChannelsUserFollows(username);

//level2
	//fetch followers of each of the channels in userFollows
		//remove followers already in cassandra from a copy of user follows
		List<String> listOfChannelsNotAlreadyInChannelsTable = removeChannelsAlreadyInDatabase(new LinkedList<String>(userFollows));
		System.out.println("after Remove channels: " + listOfChannelsNotAlreadyInChannelsTable.size());
		Map<String, List<String>> level2Map = new HashMap<String,List<String>>(); 
		//if there are any channels in userFollows, fetch them from twitch and insert to cassandra
		if(!listOfChannelsNotAlreadyInChannelsTable.isEmpty()){//as long as there is 1 channel not in the table
			//this initiates twithc api calls for things not already fetched
			level2Map =  fetchFollowersOfListOfStringChannels(listOfChannelsNotAlreadyInChannelsTable);
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

		//skip if all users were added first time around(new user no uniques edge case)
		if(level2ToFetchFromCassandra.size() > 0){
			Map<String,List<String>> map = CassandraDriver2.getChannelFollowerMap(level2ToFetchFromCassandra);
			if(map.keySet().size() > 0){
				level2Map.putAll(map);
			}
		}
			
		System.out.println("TwitchAPICallHandler: Size of level2Map: " + level2Map.keySet().size() );
		
		//level2Map should now have all followers of the channels user follows...now to test
		
//level3
		//now I need to fetch all the channels that users of level2Map have

		//first check for duplicates
		List<String> channelsInDB = CassandraDriver2.fetchAllChannelsInDatabse();
		
		


		return null;
	}

	private static List<String> fetchChannelsUserFollows(String username) {
		List<String> userFollows = null;
		
		if(!CassandraDriver2.checkIfUserChannelsFollowedAlreadyFetched(username)){
			userFollows = TwitchWrapper.getUserChannelsFollowsAsString(username);
			CassandraDriver2.insertFollowList(username, userFollows, true);
		} else{
			userFollows = CassandraDriver2.getFollowList(username);
		}
		//remove channels with more than 10,000 followers
		userFollows.removeAll(TwitchAPICallHandler.getChannelFollowersOverLong(channelFollowerCounts, followerCountCutoff));
		return userFollows;
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

	//returns list of channels from twitch API that have followers > 0
	private static Map<String, List<String>> fetchFollowersOfListOfStringChannels(List<String> listOfChannelsNotAlreadyInChannelsTable) {
		System.out.println("TwitchAPICallHandler: fetchFollowersOfListOfStringChannels " + listOfChannelsNotAlreadyInChannelsTable.size());
		Map<String, List<String>> map = new HashMap<String,List<String>>();
		List<String> workingList = null;
		int runNumber = 1;
		Iterator<String> iter = listOfChannelsNotAlreadyInChannelsTable.iterator();
		String key = "";
		Long keyFollowerCount = (long) 0;
		while(iter.hasNext()){
			key = iter.next();
			System.out.println(key + "    runNumber: " + runNumber++ + "    listOfchannelsyada.size: " + listOfChannelsNotAlreadyInChannelsTable.size());
			keyFollowerCount = TwitchWrapper.getFollowerCount(key);
			if(keyFollowerCount < followerCountCutoff && keyFollowerCount != 0){
				workingList = TwitchWrapper.getChannelFollowersAsString(key);
				if(workingList != null){
					if(workingList.size() > 0){
						map.put(key, workingList);
						CassandraDriver2.insertChannelFollowerList(key, workingList, true);
					}
				}
			} else{
				System.out.println("Skipping on: " + key + " because followerCount > " + followerCountCutoff + " or list 0");
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