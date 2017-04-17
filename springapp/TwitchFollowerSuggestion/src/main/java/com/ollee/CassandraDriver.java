package com.ollee;

import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import lombok.Getter;
import me.philippheuer.twitch4j.model.Follow;

public final class CassandraDriver {
	
	private static Cluster cluster;
	@Getter
	private static Session session;
	private static String cassandraServer = "linode.ollee.net";
	private static String keyspaceName = "twitchsuggestion";
	
	private CassandraDriver() {
	}
	
	public static void initializeCassandra(){
		System.out.println("Building Cluster");
		cluster  = Cluster.builder().addContactPoint(cassandraServer).build();
		System.out.println("connection to cluster");
		session = cluster.connect(keyspaceName);
		System.out.println("creating table if doesn't exist");
		try {
			session.execute("CREATE TABLE IF NOT EXISTS twitchsuggestion.follows(follower text PRIMARY KEY, channel text);");
		} catch (Exception e) {
			System.out.println("CREATE TABLE threw an error: " + e.getMessage());
		}
	}

	private static void deleteRow(String follower) {
		System.out.print("Attempting query:  DELETE FROM follows WHERE follower= '" + follower.toLowerCase() + "';");
		try{
			session.execute("DELETE FROM follows WHERE follower= '" + follower.toLowerCase() + "';");
		} catch (Exception e){
			System.out.println("DELETE FROM follows WHERE follower = '" + follower.toLowerCase() + "'; failed with exception: " + e.getMessage());
		}
	}
	
	public static ResultSet selectFollow(String follower) {
		System.out.println("Attempting query: SELECT * FROM follows WHERE follower='" + follower.toLowerCase() + "';");
		ResultSet result = null;
		try {
			result = session.execute("SELECT * FROM follows WHERE follower='" + follower.toLowerCase() + "' ALLOW FILTERING;");
		} catch (Exception e){
			System.out.println("SELECT " + follower.toLowerCase() + " FROM follows threw and error: " + e.getMessage());
		}
//		Iterator<Row> tempIterator = result.iterator();
//		while(tempIterator.hasNext()){
//			System.out.println(tempIterator.next().getString("channel"));
//		}
		return result;
	}
	
	private static ResultSet selectFollowerAndChannel(String follower, String channel){
		ResultSet result = null;
		System.out.println("Trying to find follow event of follower: " + follower + " and channel: " + channel);
		try {
			result = session.execute("SELECT * FROM follows WHERE follower='" + follower.toLowerCase() + "' AND channel='" + channel.toLowerCase() + "' ALLOW FILTERING;");
		} catch(Exception e){
			System.out.println("SELECT follower: " + follower.toLowerCase() + " and channel: " + channel.toLowerCase() + " from follows threw error: " + e.getMessage());
		}
		return result;
	}
	
	// keeping for support of insertFollowList(List<Follow followsList)
	public static int insertFollow(String follower, String channel){
		System.out.println("Query in insertFollow(follower,channel,date,timestamp): " + getInsertFollowStatement(follower.toLowerCase(),channel.toLowerCase()));
		try {
			session.execute(getInsertFollowStatement(follower.toLowerCase(),channel.toLowerCase()));
		} catch (Exception e) {
			System.out.println("INSERT INTO follows threw and error: " + e.getMessage());
		}
		
		return 0;
	}
	
	//this one is inefficient and only keeping incase somethingbreaks
	public static int insertFollowList(List<Follow> followsList){
		Iterator<Follow> followsListIterator = followsList.iterator();
		while(followsListIterator.hasNext()){
			Follow follow = followsListIterator.next();
			System.out.println("running insertFollow for: " + follow.getUser().getName().toLowerCase() + " for channel: " + follow.getChannel().getName().toLowerCase());
			insertFollow(follow.getUser().getName().toLowerCase(), follow.getChannel().getName().toLowerCase());
		}		
		return 0;
	}	
	// returns a insert follow query statment for follower,channel
	public static String getInsertFollowStatement(String follower, String channel){
		String statement = "";
		//Date date = new Date();
		statement = "INSERT INTO follows (uuid,follower,channel,timestamp) VALUES (" + UUID.randomUUID() + ", '" + follower.toLowerCase() + "', '" + channel.toLowerCase() + "', '" + Date.from(Instant.now()).getTime() + "')";
		//System.out.println("Generated statement: " + statement);
		return statement;
	}
	//optimal way to insert since batch inserts don't work righti n cassandra
	public static int threadedInsertFollowList(List<Follow> followsList){
		Iterator<Follow> followsListIterator = followsList.iterator();
		//this stuff eliminates inserting duplicates
		List<String> duplicates = new LinkedList<String>();
		List<Row> resultSetList= selectFollow(followsList.get(0).getUser().getName().toLowerCase()).all();
		System.out.println("Size of result set: " + resultSetList.size());
		Iterator<Row> resultSetIterator = resultSetList.iterator();
		while(resultSetIterator.hasNext()){
			String s = resultSetIterator.next().getString("channel");
			System.out.println("candidate duplicate adding to list: " + s);
			duplicates.add(s);
		}
		System.out.println("Duplicates is size: " + duplicates.size());
		//remove duplicates from followsList
		//get list of strings of follows
		List<Follow> followsToRemove = new LinkedList<Follow>();
		while(followsListIterator.hasNext()){
			Follow next = followsListIterator.next();
			if(duplicates.contains(next.getChannel().getName().toLowerCase())){
				followsToRemove.add(next);
			}
		}
		followsList.removeAll(new HashSet(followsToRemove));
		
		System.out.println("After cleanup followList is size: " + followsList.size() + "and duplicates is size" + duplicates.size());
		//insert cleaned list
		System.out.println("After cleanup of list, followsList.size(): " + followsList.size());
		followsListIterator = followsList.iterator();
		while(followsListIterator.hasNext()){
			Follow follow = followsListIterator.next();
			System.out.println("running insertFollow for: " + follow.getUser().getName().toLowerCase() + " for channel: " + follow.getChannel().getName().toLowerCase());
			ThreadedInsert thread = new ThreadedInsert(
					getInsertFollowStatement(
							follow.getUser().getName().toLowerCase(),
							follow.getChannel().getName().toLowerCase()));
			thread.start();
		}		
		return 0;
	}

	//this doesn't work, keeping for the moment for posterity
	public static void batchInsert(List<Follow> followsList){
		Iterator<Follow> i = followsList.iterator();
		String batch = "BEGIN BATCH;";
		while(i.hasNext()){
			Follow f = i.next();
			batch.concat(getInsertFollowStatement(f.getUser().getName().toLowerCase(),f.getChannel().getName().toLowerCase()) + ";\n");
		}
		batch.concat("APPLY BATCH;");
		
		try{
			session.execute(batch);
		} catch (Exception e){
			System.out.println("Exception cauth in batchInsert: " + e.getMessage());
			System.out.println(e.getStackTrace().toString());
		}
	}

}
