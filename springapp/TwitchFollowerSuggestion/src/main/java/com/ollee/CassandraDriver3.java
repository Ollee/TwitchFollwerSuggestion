package com.ollee;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;

import lombok.Getter;
import me.philippheuer.twitch4j.model.Follow;

public final class CassandraDriver3 {
	private static Cluster cluster;
	@Getter
	private static Session session;
	private static String cassandraServer = "linode.ollee.net";
	private static String keyspaceName = "twitchsuggestion";
	private static String followerTable = "followertable";
	private static String channelTable = "channeltable";
	private static String followerCacheTable = "followertablecache";
	private static Map<String, List<String>> followerTableMap = new ConcurrentHashMap<String,List<String>>();
	private static Map<String, List<String>> channelTableMap = new ConcurrentHashMap<String, List<String>>();
	
	public static void initializeCassandra(){
		System.out.println("CassandraDriver: Building Cluster");
		cluster  = Cluster.builder()
				.addContactPoint(cassandraServer)
				.withSocketOptions(new SocketOptions()
											.setConnectTimeoutMillis(25000))
				.build();
		System.out.println("CassandraDriver: connection to cluster");
		session = cluster.connect(keyspaceName);
		//TODO fetch entire DB from cassandra to local map
		fetchAllFromDatabase();
	}
	
	public static boolean followerMapContains(String username){
		return followerTableMap.containsKey(username);
	}
	private static List<String> getListFromFollowerMap(String username){
		return followerTableMap.get(username);
	}
	
	private static boolean channelMapContains(String channelname){
		return channelTableMap.containsKey(channelname);
	}
	
	private static List<String> getListFromChannelFollowerMap(String channelname){
		return channelTableMap.get(channelname);
	}

	private static void fetchAllFromDatabase() {
		System.out.println("CassandraDriver3: INITIALIZING TABLES FROM DATABASE");
		fetchAllFollowersFromDB();
		fetchAllChannelsFromDB();
		System.out.println("DATABASE LAODED");
		System.out.println("DATABASE LAODED");
		System.out.println("DATABASE LAODED");
	}

	private static void fetchAllFollowersFromDB() {
		List<Row> row1 = session.execute("SELECT count(*) from "+followerTable+";").all();
		Long size = new Long(-1);
		if(!row1.isEmpty()){
			size = row1.get(0).getLong("count");
		}
		
		Row row;
		ResultSet resultSet = session.execute("SELECT * FROM " + followerTable + ";");
		System.out.print("Followers wait Size: " + size);
		
		while(!resultSet.isFullyFetched() || !resultSet.isExhausted()){
			System.out.print("." + resultSet.getAvailableWithoutFetching() + ".");

			row = resultSet.one();
			followerTableMap.put(row.getString("username").toLowerCase(), commaSeparatedStringToStringList(row.getString("channels")));
		}
		System.out.println("CassandraDriver3: FETCHED FOLLOWERTABLEMAP: " + followerTableMap.size());
	}

	private static void fetchAllChannelsFromDB() {
		List<Row> row1 = session.execute("SELECT count(*) from "+channelTable+";").all();
		Long size = new Long(-1);
		if(!row1.isEmpty()){
			size = row1.get(0).getLong("count");
		}
		
		Row row;
		ResultSet resultSet = session.execute("SELECT * FROM " + channelTable + ";");
		System.out.print("Channels wait Size: " + size);
		
		while(!resultSet.isFullyFetched() || !resultSet.isExhausted()){
			System.out.print("." + resultSet.getAvailableWithoutFetching() + ".");

			row = resultSet.one();
			channelTableMap.put(row.getString("channelname").toLowerCase(), commaSeparatedStringToStringList(row.getString("followers")));
		}

		System.out.println("CassandraDriver3: FETCHED CHANNELTABLEMAP: " + channelTableMap.size());
		
	}
	
	public static void insertFollowList(String follower, List<String> channelsFollowed){
		System.out.println("CassandraDriver3: insertFollowLisT: " + follower);
		
		if (!followerMapContains(follower)) {
			followerTableMap.put(follower, channelsFollowed);
			try {
				session.execute("INSERT INTO " + followerTable + " (username, channels, timestamp) VALUES ('" + follower
						+ "'," + commaSeparateListFromStringList(channelsFollowed) + ", '"
						+ Date.from(Instant.now()).getTime() + "');");
			} catch (Exception e) {
				System.out.println(
						"CassandraDriver3: insertFollowList: INSERT INTO followers threw and error: " + e.getMessage());
			} 
		}
	}
	
	public static List<String> getFollowList(String follower){
		System.out.println("CassandraDriver3: getFollowList: " + follower);
//		List<String> returnList = new LinkedList<String>();
		List<Row> result = null;
		
		if(!followerMapContains(follower)){
			try{
				result = session.execute("SELECT * FROM " + followerTable + " WHERE username='" + follower + "' ALLOW FILTERING;").all();
			} catch (Exception e){
				System.out.println("CassandraDriver3: SELECT FROM followers threw an error: " + e.getStackTrace().toString());
			}
		} else{
			return getListFromFollowerMap(follower);
		}
		
		return commaSeparatedStringToStringList(result.get(0).getString("channels"));
	}
	
	private static List<String> commaSeparatedStringToStringList(String rawString){
		List<String> workingList = new LinkedList<String>(Arrays.asList(rawString.split(",")));

		List<String> lowerCaseWorkingList = new LinkedList<String>();
		Iterator<String> iter = workingList.iterator();
		while(iter.hasNext()){
			lowerCaseWorkingList.add(iter.next().toLowerCase());
		}
		
		return new LinkedList<String>(workingList);
	}
	
	private static String commaSeparateListFromFollowList(List<Follow> channelsFollowed) {
		String workingString = "'";
//		System.out.println("commaSeparateListFromFollowList: channelsfollows.size: " + channelsFollowed.size());
		Iterator<Follow> iter = channelsFollowed.iterator();
		while(iter.hasNext()){
			workingString+=iter.next().getChannel().getName().toLowerCase();
			if(iter.hasNext()){
				workingString+=",";
			} else{
				workingString+="'";
			}
		}
		
		return workingString;
	}

	public static String commaSeparateListFromStringList(List<String> channelsFollowed) {
		String workingString = "'";
//		System.out.println("CassandraDriver3: commaSeparateListFromStringList: channelsfollows.size: " + channelsFollowed.size());
		Iterator<String> iter = channelsFollowed.iterator();
		int numberofruns = 0;
		while(iter.hasNext()){
			numberofruns++;
			String next = iter.next();
			workingString+=next.toLowerCase();
			if(iter.hasNext()){
				workingString+=",";
			} else{
				workingString+="'";
			}
		}
		
//		System.out.println("after a number of runs: " + numberofruns + " workingString is: " + workingString);
		return workingString;
	}

//	public static void insertChannelFollowerList(String channelname, List<Follow> followerList){
//		System.out.println("CassandraDriver3: Inserting Channel Followers by Follow List: " + channelname);
//		try {
//			String jsonBuilder = commaSeparateListFromFollowList(followerList);
//			System.out.println("JsonBUILDER: " + jsonBuilder);
//
//			session.execute("INSERT INTO " + channelTable + " (channelname, followers, timestamp) VALUES ('" +
//								channelname + "',"+ 
//								jsonBuilder + ", '" + 
//								Date.from(Instant.now()).getTime() +"');");
//		} catch (Exception e) {
//			System.out.println("CassandraDriver3: INSERT INTO followers threw and error: " + e.getMessage());
//		}
//	}	
	
	public static void insertChannelFollowerList(String channelname, List<String> followersList){
		System.out.println("CassandraDriver3: Inserting Channel Followers by String List: " + channelname + " and followerList.size " + followersList.size());
		
		channelTableMap.put(channelname, followersList);
		
		try {
			String jsonBuilder = commaSeparateListFromStringList(followersList);

			session.execute("INSERT INTO " + channelTable + " (channelname, followers, timestamp) VALUES ('" +
								channelname + "',"+ 
								jsonBuilder + ", '" + 
								Date.from(Instant.now()).getTime() +"');");
		} catch (Exception e) {
			System.out.println("CassandraDriver3: INSERT INTO followers threw and error: " + e.getMessage());
		}
	}
	
	public static List<String> getChannelFollowerList(String channelname){
		System.out.println("CassandraDriver3: getChannelFollowerList: " + channelname);
		List<String> returnList = new LinkedList<String>();
		
		if(channelMapContains(channelname)){
			return getListFromChannelFollowerMap(channelname);
		}
		
		List<Row> result = null;
		try{
			result = session.execute("SELECT * FROM " + channelTable + " WHERE channelname='" + channelname + "' ALLOW FILTERING;").all();
		} catch (Exception e){
			System.out.println("CassandraDriver3: SELECT FROM channels threw an error: " + e.getMessage());
		}
		
		if(!result.isEmpty()){
			String testString = result.get(0).getString("followers");
			returnList = commaSeparatedStringToStringList(testString);
		}

		return returnList;
	}
	
	//technically has coverage cause it uses getChannelFollowerList
	public static Map<String, List<String>> getChannelFollowerMap(List<String> list){
		System.out.println("CassandraDriver3: getChannelFollowerMap: " + list.size());
		Map<String, List<String>> returnMap = new ConcurrentHashMap<String, List<String>>();
		Iterator<String> iter = list.iterator();
		List<String> workingList = null;
//		int counter = 0;
		String key = "";
		while(iter.hasNext()){
			key = iter.next();
			workingList = CassandraDriver3.getChannelFollowerList(key);
			if(!workingList.isEmpty()){
				returnMap.put(key, workingList);
			}
//			System.out.println("Debug Index: " + list.indexOf(key) + " run counter: " + counter++ + " Key: " + key + " returnMap: " + returnMap.size());
		}
		System.out.println("returnmap size: " + returnMap.size() + " returnmapkeysetsize: " + returnMap.keySet().size());
		return returnMap;
	}

//	public static boolean checkIfUserChannelsFollowedAlreadyFetched(String username){
//		return followerMapContains(username);
//	}
	
	//majority of this functionality was probably removed from adding local cacheing...no just return list of the keys
	public static List<String> getListOfChannelsAlreadyInDatabase() {
//		List<Row> result = null;
//		List<String> channelList = new LinkedList<String>();
//		try{
//			result = session.execute("SELECT channelname FROM " + channelTable + ";").all();
//		} catch (Exception e){
//			System.out.println("CassandraDriver3: SELECT FROM followers threw an error: " + e.getMessage());
//			return null;
//		}
//		
//		Iterator<Row> iter = result.iterator();
//		while(iter.hasNext()){
//			channelList.add(iter.next().getString("channelname"));
//		}
		
		return new LinkedList<String>(channelTableMap.keySet());
	}
	
	public static List<String> getListOfFollowersAlreadyInDatabase(){
		return new LinkedList<String>(followerTableMap.keySet());
	}
	
	public static void insertChannelFollowerCount(String channel, Long followerCount) {
		try{
			session.execute("INSERT INTO followercount (channelname, count) VALUES ('" +
				channel + "',"+ 
				followerCount + ");");
		} catch (Exception e){
			System.out.println("CassandraDriver3: SELECT FROM followers threw an error: " + e.getMessage());
		}
	}
	
	public static Map<String, Long> getChannelFollowerCounts() {
		List<Row> result = null;
		Map<String, Long> map = new ConcurrentHashMap<String,Long>();
		try{
			result = session.execute("SELECT * FROM followercount;").all();
		} catch (Exception e){
			System.out.println("CassandraDriver3: SELECT FROM followers threw an error: " + e.getMessage());
			return null;
		}
		Iterator<Row> iter = result.iterator();
		Row workingRow = null;
		while(iter.hasNext()){
			workingRow = iter.next();
			
			map.put(workingRow.getString("channelname"), workingRow.getLong("count"));
		}
		
		return map;
	}

//	public static List<String> fetchAllFollowersInDatabase(){
//		List<Row> result = null;
//		List<String> returnList = new LinkedList<String>();
//		try {
//			result = session.execute("SELECT username from " + followerTable + ";").all();
//		} catch (Exception e) {
//			
//			e.printStackTrace();
//			return null;
//		}
//		
//		Iterator<Row> iter = result.iterator();
//		Row workingRow = null;
//		while(iter.hasNext()){
//			workingRow = iter.next();
//			returnList.add(workingRow.getString("username"));
//		}
//		
//		return returnList;
//	}
}
