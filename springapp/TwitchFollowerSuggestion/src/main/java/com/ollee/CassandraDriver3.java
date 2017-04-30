package com.ollee;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;

import lombok.Getter;
import me.philippheuer.twitch4j.model.Follow;
import me.philippheuer.twitch4j.model.User;

public final class CassandraDriver3 {
	private static Cluster cluster;
	@Getter
	private static Session session;
	private static String cassandraServer = "linode.ollee.net";
	private static String keyspaceName = "twitchsuggestion";
	private static String followerTable = "followertable";
	private static String channelTable = "channelstables";
	private static String followerCacheTable = "followertablecache";
	
	public static void initializeCassandra(){
		System.out.println("CassandraDriver: Building Cluster");
		cluster  = Cluster.builder()
				.addContactPoint(cassandraServer)
				.withSocketOptions(new SocketOptions()
											.setConnectTimeoutMillis(25000))
				.build();
		System.out.println("CassandraDriver: connection to cluster");
		session = cluster.connect(keyspaceName);
		
	}

	//insert user follows
	public static void insertFollowList(String follower, List<Follow> channelsFollowed){
		System.out.println("CassandraDriver3: insertFollowLisT: " + follower);
		try {
			session.execute("INSERT INTO " + followerTable + " (username, channels, timestamp) VALUES ('" +
								follower + "',"+ 
								commaSeparateListFromFollowList(channelsFollowed) + ", '" + 
								Date.from(Instant.now()).getTime() +"');");
		} catch (Exception e) {
			System.out.println("CassandraDriver3: insertFollowList: INSERT INTO followers threw and error: " + e.getMessage());
		}
	}
	
	public static void insertFollowList(String follower, List<String> channelsFollowed, boolean bull){
		System.out.println("CassandraDriver3: insertFollowLisT: " + follower);
		try {
			session.execute("INSERT INTO " + followerTable + " (username, channels, timestamp) VALUES ('" +
								follower + "',"+ 
								commaSeparateListFromStringList(channelsFollowed) + ", '" + 
								Date.from(Instant.now()).getTime() +"');");
		} catch (Exception e) {
			System.out.println("CassandraDriver3: insertFollowList: INSERT INTO followers threw and error: " + e.getMessage());
		}
	}
	
	public static List<String> getFollowList(String follower){
		System.out.println("CassandraDriver3: getFollowList: " + follower);
//		List<String> returnList = new LinkedList<String>();
		List<Row> result = null;
		try{
			result = session.execute("SELECT * FROM " + followerTable + " WHERE username='" + follower + "' ALLOW FILTERING;").all();
		} catch (Exception e){
			System.out.println("CassandraDriver3: SELECT FROM followers threw an error: " + e.getStackTrace().toString());
		}
		
//		if(result != null && result.size() > 0){
//			System.out.println("getFollowList returned: " + result.get(0).toString());
//		} else {
//			System.out.println("getFollowList returned nothing john snuh");
//		}

//		Iterator<String> tokens = result.get(0).getSet("channels",  String.class).iterator();
//		while(tokens.hasNext()){
//			returnList.add(tokens.next());
//		}
		
		return commaSeparatedStringToStringList(result.get(0).getString("channels"));
	}
	
	private static List<String> commaSeparatedStringToStringList(String rawString){
		List<String> workingList = new LinkedList<String>(Arrays.asList(rawString.split(",")));
		 
//		System.out.println("rawString: " + rawString);
//		Iterator<String> iter = workingList.iterator();
//		System.out.print("the split list: ");
//		while(iter.hasNext()){
//			System.out.print(iter.next() + " ");
//		}
		List<String> lowerCaseWorkingList = new LinkedList<String>();
		Iterator<String> iter = workingList.iterator();
		while(iter.hasNext()){
			lowerCaseWorkingList.add(iter.next().toLowerCase());
		}
		
		
		return new LinkedList<String>(workingList);
	}

	private static String commaSeparateListFromFollowList(List<Follow> channelsFollowed) {
		String workingString = "'";
		System.out.println("commaSeparateListFromFollowList: channelsfollows.size: " + channelsFollowed.size());
		Iterator<Follow> iter = channelsFollowed.iterator();
		while(iter.hasNext()){
			workingString+=iter.next().getChannel().getName().toLowerCase();
			if(iter.hasNext()){
				workingString+=",";
			} else{
				workingString+="'";
			}
		}
		
//		System.out.println("workingString: " + workingString);
		return workingString;
	}

	public static String commaSeparateListFromStringList(List<String> channelsFollowed) {
		String workingString = "'";
		System.out.println("commaSeparateListFromStringList: channelsfollows.size: " + channelsFollowed.size());
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
		
		
		System.out.println("after a number of runs: " + numberofruns + " workingString is: " + workingString);
		return workingString;
	}

	public static void insertChannelFollowerList(String channelname, List<Follow> followerList){
		System.out.println("CassandraDriver3: Inserting Channel Followers by Follow List: " + channelname);
		try {
			String jsonBuilder = commaSeparateListFromFollowList(followerList);
			System.out.println("JsonBUILDER: " + jsonBuilder);
			
			insertIntoChannels(channelname, jsonBuilder);
//			session.execute("INSERT INTO " + channelTable + " (channelname, followers, timestamp) VALUES ('" +
//								channelname + "',"+ 
//								jsonBuilder + ", '" + 
//								Date.from(Instant.now()).getTime() +"');");
		} catch (Exception e) {
			System.out.println("CassandraDriver3: INSERT INTO followers threw and error: " + e.getMessage());
		}
	}	

	public static void insertIntoChannels(String channelName, String followersList){
		
		Statement st = new SimpleStatement("INSERT INTO channels (channelname, followers) VALUES ('" + channelName + "','" + followersList + "');");
		session.execute(st);
	}
	
	public static void insertChannelFollowerList(String channelname, List<String> followersList, boolean whatever){
		System.out.println("CassandraDriver3: Inserting Channel Followers by String List: " + channelname + " and followerList.size " + followersList.size());
		try {
			String jsonBuilder = commaSeparateListFromStringList(followersList);
//			System.out.println("JsonBUILDER: " + jsonBuilder);
			
//			insertIntoChannels(channelname, jsonBuilder);
			
			session.execute("INSERT INTO " + channelTable + " (channelname, followers, timestamp) VALUES ('" +
								channelname + "',"+ 
								jsonBuilder + ", '" + 
								Date.from(Instant.now()).getTime() +"');");
		} catch (Exception e) {
			System.out.println("CassandraDriver3: INSERT INTO followers threw and error: " + e.getMessage());
		}
	}
	
	public static void insertChannelFollowerMap(Map<String, List<String>> map){
		System.out.println("CassandraDriver3: Inserting Channel Followers by Map");
		Iterator<String> iter = map.keySet().iterator();
		String key = "";
		while(iter.hasNext()){
			key = iter.next();
			System.out.println("Inserting channel follower map for: " + key + " with followers#: " + map.get(key).size());
			CassandraDriver3.insertChannelFollowerList(key, map.get(key), true);
		}
	}
	
	public static List<String> getChannelFollowerList(String channelname){
		System.out.println("CassandraDriver3: getChannelFollowerList: " + channelname);
		List<String> returnList = new LinkedList<String>();
		List<Row> result = null;
		try{
			result = session.execute("SELECT * FROM " + channelTable + " WHERE channelname='" + channelname + "' ALLOW FILTERING;").all();
		} catch (Exception e){
			System.out.println("CassandraDriver3: SELECT FROM channels threw an error: " + e.getMessage());
		}
		
		if(!result.isEmpty()){
//			System.out.println("result tostring: " + result.get(0).toString());
			String testString = result.get(0).getString("followers");
//			System.out.println("testString: " + testString);
			returnList = commaSeparatedStringToStringList(testString);
		}
		
//		System.out.println("returnList Size: " + returnList.size());
		return returnList;
	}
	
	public static Map<String, List<String>> getChannelFollowerMap(List<String> list){
		System.out.println("CassandraDriver3: getChannelFollowerMap: " + list.size());
		Map<String, List<String>> returnMap = new HashMap<String, List<String>>();
		Iterator<String> iter = list.iterator();
		List<String> workingList = null;
		int counter = 0;
		String key = "";
		while(iter.hasNext()){
			key = iter.next();

			workingList = CassandraDriver3.getChannelFollowerList(key);
//			System.out.println("Workinglist: " + workingList.size());
			if(!workingList.isEmpty()){
				returnMap.put(key, workingList);
			}
			
			System.out.println("Debug Index: " + list.indexOf(key) + " run counter: " + counter++ + " Key: " + key + " returnMap: " + returnMap.size());
			
		}
		System.out.println("returnmap size: " + returnMap.size() + " returnmapkeysetsize: " + returnMap.keySet().size());
		return returnMap;
	}
	
	public static boolean checkIfChannelFollowersAlreadyFetched(String channelName){
		List<Row> result = null;
		try{
			result = session.execute("SELECT channelname FROM " + channelTable + " WHERE channelname='" + channelName + "' ALLOW FILTERING;").all();
		} catch (Exception e){
			System.out.println("CassandraDriver3: SELECT FROM followers threw an error: " + e.getMessage());
		}
		
		if(!result.isEmpty() && result.get(0).getString("channelname").toLowerCase().equals(channelName.toLowerCase())){
			return true;
		}
		return false;
	}

	public static boolean checkIfUserChannelsFollowedAlreadyFetched(String username){
		List<Row> result = null;
		try{
			result = session.execute("SELECT username FROM " + followerTable + " WHERE username='" + username + "' ALLOW FILTERING;").all();
		} catch (Exception e){
			System.out.println("CassandraDriver3: SELECT FROM followers threw an error: " + e.getMessage());
		}
		
		if(!result.isEmpty() && result.get(0).getString("username").toLowerCase().equals(username.toLowerCase())){
			return true;
		}
		return false;
	}

	public static List<String> getListOfChannelsAlreadyInDatabase() {
		List<Row> result = null;
		List<String> channelList = new LinkedList<String>();
		try{
			result = session.execute("SELECT channelname FROM " + channelTable + ";").all();
		} catch (Exception e){
			System.out.println("CassandraDriver3: SELECT FROM followers threw an error: " + e.getMessage());
			return null;
		}
		
		Iterator<Row> iter = result.iterator();
		while(iter.hasNext()){
			channelList.add(iter.next().getString("channelname"));
		}
		
		return channelList;
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
		Map<String, Long> map = new HashMap<String,Long>();
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

	public static List<String> fetchAllChannelsInDatabse() {
		List<Row> result = null;
		List<String> returnList = new LinkedList<String>();
		try {
			result = session.execute("SELECT channelname from " + channelTable + ";").all();
		} catch (Exception e) {
			
			e.printStackTrace();
			return null;
		}
		
		Iterator<Row> iter = result.iterator();
		Row workingRow = null;
		while(iter.hasNext()){
			workingRow = iter.next();
			returnList.add(workingRow.getString("channelname"));
		}
		
		return returnList;
	} 
	
	public static List<String> fetchAllFollowersInDatabase(){
		List<Row> result = null;
		List<String> returnList = new LinkedList<String>();
		try {
			result = session.execute("SELECT username from " + followerTable + ";").all();
		} catch (Exception e) {
			
			e.printStackTrace();
			return null;
		}
		
		Iterator<Row> iter = result.iterator();
		Row workingRow = null;
		while(iter.hasNext()){
			workingRow = iter.next();
			returnList.add(workingRow.getString("username"));
		}
		
		return returnList;
	}
	
	public static void insertIntoFollowerCacheTable(String followerName){
		try{
			session.execute("INSERT INTO " + followerCacheTable + "(username, timestamp) values ('"
															+followerName+ "','" + Date.from(Instant.now()).getTime() + "');");
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static List<String> getFollowerCacheTable(){
	
		return null;
	}
	
	public static void insertIntoChannelCacheTable(String channelName){
		
	}
	
	public static List<String> getChannelCacheTable(){
		
		return null;
	}

	
}
















