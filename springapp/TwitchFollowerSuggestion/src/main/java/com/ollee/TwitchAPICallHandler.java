package com.ollee;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public final class TwitchAPICallHandler {
	
	private static List<String> channelsAlreadyInChannelsDatabase = new LinkedList<String>();
	private static Map<String, Long> channelFollowerCounts = new ConcurrentHashMap<String, Long>();
	private static Long followerCountCutoff = new Long(3000);

	public TwitchAPICallHandler(){
	}
	
	public LinkedHashMap<String, Integer> fetchChannelSuggestions(String username){
		
		//Level 1: userFollows contains a list of all channels a user follows
		//Level 2: level2Map contains a clean map of all followers of the channels a user follows and their followers
		//Level 3: level3Map contains a map of all those follows and the channels they follow
		
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
		
		Map<String, List<String>> level2Map = new ConcurrentHashMap<String,List<String>>(); 
		
		//if there are any channels in userFollows, fetch them from twitch and insert to cassandra
		if(!listOfChannelsNotAlreadyInChannelsTable.isEmpty()){//as long as there is 1 channel not in the table
			
			//this initiates Twitch API calls for things not already fetched
			System.out.println("TwitchAPICallHandler: There are currently: " + listOfChannelsNotAlreadyInChannelsTable.size() + " channels not already in database");
			level2Map =  fetchFollowersOfListOfStringChannels(listOfChannelsNotAlreadyInChannelsTable);
			//so if this runs, level2Map has queries already run to twitch and inserted into cassandra
		} 
		
		//end the block fetching channels from twitchAPI to level2map
		
		//this block fetches channels already in cassandra database
		
		List<String> listOfChannelsAlreadyInChannelsTable = new LinkedList<String>();
		//create list of remaining to fetch from cassandra
		
		if(level2Map.isEmpty()){
			//if map is empty candidate list = user follows
			System.out.println("TwitchAPICallHandler: level2Map is empty, no channels were fetched from twitch api");
			
			listOfChannelsAlreadyInChannelsTable = new LinkedList<String>(userFollows);
		} else{
			//if map not empty, create candidate list to fetch from cassandra
			System.out.println("TwitchAPICallHandler: level2Map is not empty, channels were fetched from API, fetching others");
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
		
		//level2Map should now have all followers of the channels user follows
		
//level3
		//now I need to fetch all the channels that users of level2Map have

		//first check for duplicates
		List<String> channelsInDB = CassandraDriver3.getListOfFollowersAlreadyInDatabase();
		
		System.out.println("TwitchAPICallHandler: Channels already in DB after run: " + channelsInDB.size());

		//translate level2map into list of followers to fetch channels they follow
		
		//clean that list of channelsInDB
		List<String> level3ChannelsToFetch = removeChannelsAlreadyInDatabase(new LinkedList<String>(level2Map.keySet()), channelsInDB);
		
		//fetch channels followed by remaining users in list
		int outerCountdown = 0;
		int innerCountdown = 0;
		
		System.out.println("TwitchAPICallHandler: level3ChannelsToFetch: " + level3ChannelsToFetch.size());
		
		Map<String, List<String>> level3Map = new ConcurrentHashMap<String,List<String>>();
		
		Iterator<String> level3Iter = level3ChannelsToFetch.iterator();
		Iterator<String> internalLevel3Iter;
		
		String dumpString = "";
		String internalDumpString = "";

		outerCountdown = level3ChannelsToFetch.size();
		
		while(level3Iter.hasNext()){
//			System.out.println("TwitchAPICallHandler: pass of first loops over level2map keys");
			
			dumpString = level3Iter.next();
			internalLevel3Iter = level2Map.get(dumpString).iterator();
			innerCountdown = level2Map.get(dumpString).size();
			
			while(internalLevel3Iter.hasNext()){
				internalDumpString = internalLevel3Iter.next();
				
				if (!level3Map.containsKey(internalDumpString)) {
					//				System.out.println("TwitchAPICallHandler: pass of second loop over the key list");
					
					if (internalDumpString != null) {
						
						level3Map.put(internalDumpString, fetchChannelsUserFollows(internalDumpString));
						System.out.println("TwitchAPICallHandler: handled " + internalDumpString + "on run: " + innerCountdown
								+ " outercountdown: " + outerCountdown);
					}
					innerCountdown--;
				}
			}
			System.out.println("TwitchAPICallHandler: handled " + dumpString + " on run: " + outerCountdown);
			outerCountdown--;
		}
		
		System.out.println("THIS SHOULD BE IT: " + level3Map.size());
	
//done with data collection, now to calculate weights
		
		//calculate weight of channel suggestion by mutually followed channels Map<follower,channelname> then count occurances of channelname
			//this is the weighting of channel suggestions
			//create a <string,int> that is followers with mutual channels followed // note original user follows are in userFollows
		
		Map<String, Integer> commonCount = new ConcurrentHashMap<String, Integer>();
		
		//reuse iterator
		level3Iter = level3Map.keySet().iterator();
		
		List<String> destroyableListOfChannels;
		String level3Key;
		String internalDump;
		
		while(level3Iter.hasNext()){
			
			level3Key = level3Iter.next();
			destroyableListOfChannels = new LinkedList<String>(level3Map.get(level3Key));
			internalLevel3Iter = destroyableListOfChannels.iterator();
			
			while(internalLevel3Iter.hasNext()){
				internalDump = internalLevel3Iter.next();
				
				if(!commonCount.containsKey(internalDump)){
					
					commonCount.put(internalDump, 1);
				} else{
					int dummy = commonCount.get(internalDump)+1;
					
					commonCount.put(internalDump, dummy);
				}
			}
		}
		
		//common count now has a list of channels and how many people who follow your channels follow them
		
		destroyableListOfChannels = new LinkedList<String>(commonCount.keySet());
		destroyableListOfChannels.retainAll(userFollows);
		
		level3Iter = destroyableListOfChannels.iterator();
		
		while(level3Iter.hasNext()){
			commonCount.remove(level3Iter.next());
		}
		
		//now have commonCount as a list of channels and ints of mutually followed channels
		//sort that map
		
		Entry<String,Integer> sortedEntry;
		
		List<Entry<String, Integer>> holdingSorted = entriesSortedByValues(commonCount);
		Iterator<Entry<String, Integer>> sortedIterator = holdingSorted.iterator();
		LinkedHashMap<String,Integer> sortedCommonCount = new LinkedHashMap<String,Integer>();
		
		int finalCounter = 0;
		
		while(sortedIterator.hasNext() && finalCounter < 25){
			System.out.println("TwitchAPICallHandler: in final loop: " + finalCounter);
			
			sortedEntry = sortedIterator.next();
			
			if(sortedEntry.getKey() != username && !userFollows.contains(sortedEntry.getKey())){
				if(channelFollowerCounts.containsKey(sortedEntry.getKey()) && sortedEntry.getValue() < followerCountCutoff){
					System.out.println("DEBUG: " + sortedEntry.getKey() + " " + sortedEntry.getValue());
					
					sortedCommonCount.put(sortedEntry.getKey(), sortedEntry.getValue());
					finalCounter++;
					
				} else {
					System.out.println("TwitchAPICallHandler: Hitting API for follower Count");
					Long tempLong = TwitchWrapper.getFollowerCount(sortedEntry.getKey());
					
					addChannelFollowerCounts(sortedEntry.getKey(), tempLong);
					
					if(tempLong < followerCountCutoff){
						System.out.println("DEBUG: " + sortedEntry.getKey() + " " + sortedEntry.getValue());
						
						sortedCommonCount.put(sortedEntry.getKey(), sortedEntry.getValue());
						finalCounter++;
					}
				}
			}
			
		}

		return sortedCommonCount;
	}
	
	// got this from http://stackoverflow.com/questions/11647889/sorting-the-mapkey-value-in-descending-order-based-on-the-value
	private static <K,V extends Comparable<? super V>> List<Entry<K, V>> entriesSortedByValues(Map<K,V> map) {

		List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(map.entrySet());

		Collections.sort(sortedEntries, 
		    new Comparator<Entry<K,V>>() {
		        @Override
		        public int compare(Entry<K,V> e1, Entry<K,V> e2) {
		            return e2.getValue().compareTo(e1.getValue());
		        }
		    }
		);

		return sortedEntries;
	}

	private static List<String> removeChannelsAlreadyInDatabase(List<String> level3ChannelsToFetch, List<String> channelsInDB) {

		System.out.println("CHANNELSINDB: " + channelsInDB.size());
		System.out.println("BEFORE REMOVAL: " + level3ChannelsToFetch.size());

		if(!channelsInDB.isEmpty() && channelsInDB != null){
			level3ChannelsToFetch.removeAll(channelsInDB);
		}
		
		System.out.println("\nAFTER REMOVAL: " + level3ChannelsToFetch.size() + " and it ran: " + " times\n\n");
		
		return level3ChannelsToFetch;
	}

	private static List<String> fetchChannelsUserFollows(String username) {
		List<String> userFollows = new LinkedList<String>();
		
		if(!CassandraDriver3.followerMapContains(username)){
			//if not in the database, fetch from twitch
			try {
				userFollows = TwitchWrapper.getUserChannelsFollowsAsString(username);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("TwitchAPICallHandler: EXCEPTION");
			}
			
			System.out.println("TwitchAPICallHandler: Called TwitchAPI: " + username);
			CassandraDriver3.insertFollowList(username, userFollows); //insert in to cassandra
			
		} else{// else get from databse
			userFollows = CassandraDriver3.getFollowList(username);
		}
		//remove channels with more than 3,000 followers
		//TODO move this functionality into the driver
		userFollows.removeAll(TwitchAPICallHandler.getChannelFollowersOverLong(channelFollowerCounts, followerCountCutoff));
		
		return userFollows;
	}

	private static List<String> getChannelFollowersOverLong(Map<String, Long> map, Long cutoff) {
		String key;
		List<String> toRemove = new LinkedList<String>();
		Iterator<String> iter = map.keySet().iterator();
		
		while(iter.hasNext()){
			key = iter.next();
			
			if(map.get(key) > cutoff || map.get(key) == new Long(0)){
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
		Map<String, List<String>> map = new ConcurrentHashMap<String,List<String>>();
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
						CassandraDriver3.insertChannelFollowerList(key, workingList);
					}
				}
			} else{
				System.out.println("Skipping on: " + key + " because followerCount > " + followerCountCutoff + " or list 0");
			}
		}
	
		
		return map;
	}



	private static List<String> removeChannelsAlreadyInDatabase(List<String> linkedList) {
		System.out.println("TwitchAPICallHandler: removeChannelsAlreadyInDatabase: " + linkedList.size());

		linkedList.removeAll(channelsAlreadyInChannelsDatabase);

		return linkedList;
	}

	public static void addToChannelFollowerCounts(String channel, Long followerCount) {
		channelFollowerCounts.put(channel, followerCount);
	}

	public static void addChannelFollowerCounts(String channelname, Long count){
		CassandraDriver3.insertChannelFollowerCount(channelname, count);
		channelFollowerCounts.put(channelname, count);
	}
}
