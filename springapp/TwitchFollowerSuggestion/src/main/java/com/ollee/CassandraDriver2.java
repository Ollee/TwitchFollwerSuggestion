package com.ollee;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.transport.DataType;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

import lombok.Getter;
import me.philippheuer.twitch4j.model.Channel;
import me.philippheuer.twitch4j.model.Follow;
import me.philippheuer.twitch4j.model.User;

public final class CassandraDriver2 {
	private static Cluster cluster;
	@Getter
	private static Session session;
	private static String cassandraServer = "linode.ollee.net";
	private static String keyspaceName = "twitchsuggestion";
	private static String followerTable = "followers";
	private static String channelTable = "channels";
	
	public static void initializeCassandra(){
		System.out.println("CassandraDriver: Building Cluster");
		cluster  = Cluster.builder()
				.addContactPoint(cassandraServer)
				.withSocketOptions(new SocketOptions()
											.setConnectTimeoutMillis(25000))
				.build();
		System.out.println("CassandraDriver: connection to cluster");
		session = cluster.connect(keyspaceName);
		System.out.println("CassandraDriver: creating table if doesn't exist");
		try {
			session.execute("CREATE TABLE IF NOT EXISTS twitchsuggestion." + followerTable + 
					" (username TEXT PRIMARY KEY, channels set<text>, timestamp timestamp);");
		} catch (Exception e) {
			System.out.println("CassandraDriver2: CREATE TABLE threw an error: " + e.getMessage());
		}
	}

	//insert user follows
	public static void insertFollowList(String follower, List<Follow> channelsFollowed){
		System.out.println("CassandraDriver2: insertFollowLisT: " + follower);
		try {
			session.execute("INSERT INTO " + followerTable + " (username, channels, timestamp) VALUES ('" +
								follower + "',"+ 
								jsonFollowingListString(channelsFollowed) + ", '" + 
								Date.from(Instant.now()).getTime() +"');");
		} catch (Exception e) {
			System.out.println("CassandraDriver2: insertFollowList: INSERT INTO followers threw and error: " + e.getMessage());
		}
	}
	
	public static void insertFollowList(String follower, List<String> channelsFollowed, boolean bull){
		System.out.println("CassandraDriver2: insertFollowLisT: " + follower);
		try {
			session.execute("INSERT INTO " + followerTable + " (username, channels, timestamp) VALUES ('" +
								follower + "',"+ 
								jsonStringListString(channelsFollowed) + ", '" + 
								Date.from(Instant.now()).getTime() +"');");
		} catch (Exception e) {
			System.out.println("CassandraDriver2: insertFollowList: INSERT INTO followers threw and error: " + e.getMessage());
		}
	}
	
	public static List<String> getFollowList(String follower){
		System.out.println("CassandraDriver2: getFollowList: " + follower);
		List<String> returnList = new LinkedList<String>();
		List<Row> result = null;
		try{
			result = session.execute("SELECT * FROM " + followerTable + " WHERE username='" + follower + "' ALLOW FILTERING;").all();
		} catch (Exception e){
			System.out.println("CassandraDriver2: SELECT FROM followers threw an error: " + e.getStackTrace().toString());
		}
		
		if(result != null && result.size() > 0){
			System.out.println("getFollowList returned: " + result.get(0).toString());
		} else {
			System.out.println("getFollowList returned nothing john snuh");
		}

		Iterator<String> tokens = result.get(0).getSet("channels",  String.class).iterator();
		while(tokens.hasNext()){
			returnList.add(tokens.next());
		}
		
		return returnList;
	}

	private static String jsonFollowingListString(List<Follow> channelsFollowed) {
		String workingString = "{";
		System.out.println("jsonfollowingstring: channelsfollows.size: " + channelsFollowed.size());
		Iterator<Follow> iter = channelsFollowed.iterator();
		while(iter.hasNext()){
			workingString+="'";
			workingString+=iter.next().getChannel().getName().toLowerCase();
			if(iter.hasNext()){
				workingString+="',";
			} else{
				workingString+="'}";
			}
		}
		
//		System.out.println("workingString: " + workingString);
		return workingString;
	}

	private static String jsonStringListString(List<String> channelsFollowed) {
		String workingString = "{";
		System.out.println("jsonfollowingstring: channelsfollows.size: " + channelsFollowed.size());
		Iterator<String> iter = channelsFollowed.iterator();
		while(iter.hasNext()){
			workingString+="'";
			workingString+=iter.next().toLowerCase();
			if(iter.hasNext()){
				workingString+="',";
			} else{
				workingString+="'}";
			}
		}
		
//		System.out.println("workingString: " + workingString);
		return workingString;
	}

	public static void insertChannelFollowerList(String follower, List<Follow> channelsFollowed){
		System.out.println("CassandraDriver2: Inserting Channel Followers by Follow List: " + follower);
		try {
			session.execute("INSERT INTO " + channelTable + " (channelname, followers, timestamp) VALUES ('" +
								follower + "',"+ 
								jsonFollowingListString(channelsFollowed) + ", '" + 
								Date.from(Instant.now()).getTime() +"');");
		} catch (Exception e) {
			System.out.println("CassandraDriver2: INSERT INTO followers threw and error: " + e.getMessage());
		}
	}	

	public static void insertChannelFollowerList(String follower, List<String> channelsFollowed, boolean whatever){
		System.out.println("CassandraDriver2: Inserting Channel Followers by String List: " + follower);
		try {
			session.execute("INSERT INTO " + channelTable + " (channelname, followers, timestamp) VALUES ('" +
								follower + "',"+ 
								jsonStringListString(channelsFollowed) + ", '" + 
								Date.from(Instant.now()).getTime() +"');");
		} catch (Exception e) {
			System.out.println("CassandraDriver2: INSERT INTO followers threw and error: " + e.getMessage());
		}
	}
	
	public static void insertChannelFollowerMap(Map<String, List<String>> map){
		System.out.println("CassandraDriver2: Inserting Channel Followers by Map");
		Iterator<String> iter = map.keySet().iterator();
		String key = "";
		while(iter.hasNext()){
			key = iter.next();
			CassandraDriver2.insertChannelFollowerList(key, map.get(key), true);
		}
	}
	
	public static List<String> getChannelFollowerList(String follower){
		System.out.println("CassandraDriver2: getChannelFollowerList: " + follower);
		List<String> returnList = new LinkedList<String>();
		List<Row> result = null;
		try{
			result = session.execute("SELECT * FROM " + channelTable + " WHERE channelname='" + follower + "' ALLOW FILTERING;").all();
		} catch (Exception e){
			System.out.println("CassandraDriver2: SELECT FROM channels threw an error: " + e.getMessage());
		}
		
		if(result != null && result.size() > 0){
			System.out.println("getChannelFollowerList returned: " + result.get(0).toString());
		} else {
			System.out.println("getChannelFollowerList returned nothing john snuh");
		}

		Iterator<String> tokens = result.get(0).getSet("followers",  String.class).iterator();
		while(tokens.hasNext()){
			returnList.add(tokens.next());
		}
		
		return returnList;
	}
	
	public static Map<String, List<String>> getChannelFollowerMap(List<String> list){
		Map<String, List<String>> returnMap = new HashMap<String, List<String>>();
		Iterator<String> iter = list.iterator();
		String key = "";
		while(iter.hasNext()){
			key = iter.next();
			returnMap.put(key, CassandraDriver2.getChannelFollowerList(key));
		}
		
		return returnMap;
	}
	
	public static boolean checkIfChannelFollowersAlreadyFetched(String channelName){
		List<Row> result = null;
		try{
			result = session.execute("SELECT channelname FROM " + channelTable + " WHERE channelname='" + channelName + "' ALLOW FILTERING;").all();
		} catch (Exception e){
			System.out.println("CassandraDriver2: SELECT FROM followers threw an error: " + e.getMessage());
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
			System.out.println("CassandraDriver2: SELECT FROM followers threw an error: " + e.getMessage());
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
			System.out.println("CassandraDriver2: SELECT FROM followers threw an error: " + e.getMessage());
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
			System.out.println("CassandraDriver2: SELECT FROM followers threw an error: " + e.getMessage());
		}
	}

	public static Map<String, Long> getChannelFollowerCounts() {
		List<Row> result = null;
		Map<String, Long> map = new HashMap<String,Long>();
		try{
			result = session.execute("SELECT * FROM followercount;").all();
		} catch (Exception e){
			System.out.println("CassandraDriver2: SELECT FROM followers threw an error: " + e.getMessage());
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

	
}
















