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
		
		// userFollows contains a list of all channels a user follows
		//level2Map contains a clean map of all followers of the channels a user follows and their followers
		//level3Map contains a map of all those follows and the channels they follow
		
		channelsAlreadyInChannelsDatabase = CassandraDriver3.getListOfChannelsAlreadyInDatabase();
		
		TwitchAPICallHandler.updateChannelFollowerCounts();
		
//level1
	//fetch channels user follows
		List<String> userFollows = fetchChannelsUserFollows(username);

//level2
	//fetch followers of each of the channels in userFollows
		//this block fetches channels from twitch API to level2map
		//remove followers already in cassandra from a copy of user follows
		List<String> listOfChannelsNotAlreadyInChannelsTable = removeChannelsAlreadyInDatabase(new LinkedList<String>(userFollows));
		System.out.println("TwitchAPICallHandler: after Remove channels: " + listOfChannelsNotAlreadyInChannelsTable.size());
		Map<String, List<String>> level2Map = new HashMap<String,List<String>>(); 
		//if there are any channels in userFollows, fetch them from twitch and insert to cassandra
		if(!listOfChannelsNotAlreadyInChannelsTable.isEmpty()){//as long as there is 1 channel not in the table
			//this initiates twithc api calls for things not already fetched
			System.out.println("TwitchAPICallHandler: There are currently: " + listOfChannelsNotAlreadyInChannelsTable.size() + " channels not already in database");
			level2Map =  fetchFollowersOfListOfStringChannels(listOfChannelsNotAlreadyInChannelsTable);
			//so if this runs, level2Map has queries already run to twitch and inserted into cassandra
		} 
		//end the block fetching channels from twitchAPI to level2map
		
		//this block fetches channels already in cassandra database
		
		List<String> listOfChannelsAlreadyInChannelsTable = new LinkedList<String>();
		//create list of remaining to fetch from cassandra
		if(level2Map.isEmpty()){
			System.out.println("TwitchAPICallHandler: level2Map is empty, no channels were fetched from twitch api");
			//if map is empty candidate list = user follows
			listOfChannelsAlreadyInChannelsTable = new LinkedList<String>(userFollows);
		} else{
			System.out.println("TwitchAPICallHandler: level2Map is not empty, channels were fetched from API, fetching others");
			//if map not empty, create candidate list to fetch from cassandra
			List<String> copyOfUserFollows = new LinkedList<String>(userFollows);
			copyOfUserFollows.removeAll(level2Map.keySet()); //remove all keys in level2Map from copyOfUserFollows
			listOfChannelsAlreadyInChannelsTable.addAll(copyOfUserFollows);
			System.out.println("TwitchAPICallHandler: level2map not empty: copyOfUserFollowsAfterREmove: " + copyOfUserFollows.size());
			System.out.println("TwitchAPICallHandler: level2map not empty: number of channels already in database: " + listOfChannelsAlreadyInChannelsTable.size());
		}

		//skip if all users were added first time around(new user no uniques edge case)
		if(listOfChannelsAlreadyInChannelsTable.size() > 0){
			System.out.println("TwitchAPICallHandler: level2ToFetchFromCassandra is size: " + listOfChannelsAlreadyInChannelsTable.size());
			Map<String,List<String>> map = CassandraDriver3.getChannelFollowerMap(listOfChannelsAlreadyInChannelsTable);
			System.out.println("IMPORTANT DEBUG: " + map.size() + " more important keysetsize: " + map.keySet().size());
			if(map.keySet().size() > 0){
				System.out.println("TwitchAPICallHandler: map from with channel followers list is size: " + map.keySet().size());
				level2Map.putAll(map);
			}
		}
			
		System.out.println("TwitchAPICallHandler: Size of level2Map: " + level2Map.keySet().size() );
		
		//level2Map should now have all followers of the channels user follows...now to test
		
//level3
		//now I need to fetch all the channels that users of level2Map have

		//first check for duplicates
		List<String> channelsInDB = CassandraDriver3.fetchAllFollowersInDatabase();
		
		System.out.println("TwitchAPICallHandler: Channels already in DB after run: " + channelsInDB.size());

		//translate level2map into list of followers to fetch channels they follow
		
		//clean that list of channelsInDB
		List<String> level3ChannelsToFetch = removeChannelsAlreadyInDatabase(new LinkedList<String>(level2Map.keySet()), channelsInDB);
		
		//fetch channels followed by remaining users in list
		int outerCountdown = 0;
		int innerCountdown = 0;
		
		System.out.println("TwitchAPICallHandler: level3ChannelsToFetch: " + level3ChannelsToFetch.size());
		Map<String, List<String>> level3Map = new HashMap<String,List<String>>();
		Iterator<String> level3Iter = level3ChannelsToFetch.iterator();
		outerCountdown = level3ChannelsToFetch.size();
		String dumpString = "";
		String internalDumpString = "";
		Iterator<String> internalLevel3Iter;
		while(level3Iter.hasNext()){
			System.out.println("TwitchAPICallHandler: pass of first loops over level2map keys");
			dumpString = level3Iter.next();
			internalLevel3Iter = level2Map.get(dumpString).iterator();
			innerCountdown = level2Map.get(dumpString).size();
			while(internalLevel3Iter.hasNext()){
				System.out.println("TwitchAPICallHandler: pass of second loop over the key list");
				internalDumpString = internalLevel3Iter.next();
				if(internalDumpString != null){
					level3Map.put(internalDumpString, fetchChannelsUserFollows(internalDumpString));
					System.out.println("TwitchAPICallHandler: handled " + internalDumpString + "on run: " + innerCountdown + " outercountdown: " + outerCountdown);
				}
				innerCountdown--;
			}
			System.out.println("TwitchAPICallHandler: handled " + dumpString + " on run: " + outerCountdown);
			outerCountdown--;
		}
		
		System.out.println("THIS SHOULD BE IT: " + level3Map.size());
		
		//calculate weight of channel suggestion by mutually followed channels Map<follower,channelname> then count occurances of channelname
			//this is the weighting of channel suggestions
		
		return null;
	}

	private static List<String> removeChannelsAlreadyInDatabase(List<String> level3ChannelsToFetch, List<String> channelsInDB) {

		System.out.println("CHANNELSINDB: " + channelsInDB.size());
		System.out.println("BEFORE REMOVAL: " + level3ChannelsToFetch.size());
//		int existscounter = 0;
//		String dump;
//		int channelsRemoved = 0;
//		Iterator<String> iter = channelsInDB.iterator();
//		while(iter.hasNext()){
//			dump = iter.next();//get item from channelsindb
//			int index = -1;//reset index
//			Iterator<String> iter2 = level3ChannelsToFetch.iterator();//create fresh level3 iterator
//			int innerIndex = 0;//reset inner index
//			while(iter2.hasNext() && index == -1){//while things left in leve3 
//				String dump2 = iter2.next();//get next item in level3 iterator
//				if(dump2.equals(dump)){//if the item from level3 == channelsindb
//					index = innerIndex;//set index and break
//					channelsRemoved++;
//				}
//				innerIndex++;//increment inner if we didn't break
//			}
//			if(index >= 0){//if we broke
//				level3ChannelsToFetch.remove(index);//remove at the break
//			}
//			existscounter++;
//		}
//		List<String> retainList = new LinkedList<String>(level3ChannelsToFetch);
//		retainList.retainAll(channelsInDB);
//		System.out.println("RETAIN LIST SIZE: " + retainList.size());
//		
//		System.out.println("DUMPING LISTS");
//		
//		int counter1 = level3ChannelsToFetch.size()-1;
//		int counter2 = channelsInDB.size()-1;
//		
//		while(counter1 >= 0 || counter2 >= 0){
//			if(counter1 >=0){
//				System.out.print(level3ChannelsToFetch.get(counter1));
//			} else{
//				System.out.print("empty");
//			}
//			if(counter2 >=0){
//				System.out.print("," + channelsInDB.get(counter2) + "\n");
//			} else{
//				System.out.print(",empty\n");
//			}
//			counter1--;
//			counter2--;
//		}
		
		level3ChannelsToFetch.removeAll(channelsInDB);
		
		System.out.println("\nAFTER REMOVAL: " + level3ChannelsToFetch.size() + " and it ran: " + " times\n\n");
		
		return level3ChannelsToFetch;
	}

	private static Map<String, List<String>> cleanMapOfDuplicateChannels(Map<String, List<String>> map,
																		List<String> list) {
		System.out.println("TwitchAPICallHandler: cleanMapOfDuplicateChannels: map.size: " + map + " list.size: " + list.size());
		String workingString = "";
		Iterator<String> iter = list.iterator();
		while(iter.hasNext()){
			workingString = iter.next();
			if(map.containsKey(workingString)){
				map.remove(workingString);
			}
		}
		
		return map;
	}

	private static List<String> fetchChannelsUserFollows(String username) {
		List<String> userFollows = null;
		boolean exception = false;
		if(!CassandraDriver3.checkIfUserChannelsFollowedAlreadyFetched(username)){
			try {
				userFollows = TwitchWrapper.getUserChannelsFollowsAsString(username);
			} catch (Exception e) {
				exception = true;
				e.printStackTrace();
				System.out.println("TwitchAPICallHandler: EXCEPTION");
			}
			System.out.println("TwitchAPICallHandler: Called TwitchAPI: " + username);
			CassandraDriver3.insertFollowList(username, userFollows, true);
		} else{
			userFollows = CassandraDriver3.getFollowList(username);
		}
		if(exception){
			userFollows = new LinkedList<String>();
		}else {
			//remove channels with more than 10,000 followers
			userFollows.removeAll(TwitchAPICallHandler.getChannelFollowersOverLong(channelFollowerCounts, followerCountCutoff));
		}
		return userFollows;
	}

	private static List<String> getChannelFollowersOverLong(Map<String, Long> map, Long cutoff) {
		String key;
		List<String> toRemove = new LinkedList<String>();
		Iterator<String> iter = map.keySet().iterator();
		while(iter.hasNext()){
			key = iter.next();
			if(map.get(key) > cutoff){
//				System.out.println("Rmemoving : " + key + " from list");
				toRemove.add(key);
			}
		}
		return toRemove;
	}

	private static void updateChannelFollowerCounts() {
		channelFollowerCounts = CassandraDriver3.getChannelFollowerCounts();
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
			System.out.println("fetchFollowersOfListOfStringChannels: " + key + "    runNumber: " + runNumber++ + "    listOfchannelsyada.size: " + listOfChannelsNotAlreadyInChannelsTable.size());
			keyFollowerCount = TwitchWrapper.getFollowerCount(key);
			if(keyFollowerCount < followerCountCutoff && keyFollowerCount != 0){
				workingList = TwitchWrapper.getChannelFollowersAsString(key);
				System.out.println("TwitchAPICallHandler: fetchFollowersOfListOfStringChannels: workingList.size(): " + workingList.size());
				if(workingList != null){
					if(workingList.size() > 0){
						map.put(key, workingList);
						CassandraDriver3.insertChannelFollowerList(key, workingList, true);
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
//			if(CassandraDriver3.checkIfChannelFollowersAlreadyFetched(str)){
//				toRemove.add(str);
//			}
//		}
		
		linkedList.removeAll(channelsAlreadyInChannelsDatabase);
//		linkedList.removeAll(toRemove);
//		
//		Iterator<String> iterator = linkedList.iterator();
//		while (iterator.hasNext()){
//			String next = iterator.next();
//			if(CassandraDriver3.checkIfChannelFollowersAlreadyFetched(next.toLowerCase())){
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