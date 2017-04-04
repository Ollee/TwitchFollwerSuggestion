package com.ollee;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
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

	private void deleteRow(String follower) {
		System.out.print("Attempting query:  DELETE FROM follows WHERE follower= '" + follower.toLowerCase() + "';");
		try{
			session.execute("DELETE FROM follows WHERE follower= '" + follower.toLowerCase() + "';");
		} catch (Exception e){
			System.out.println("DELETE FROM follows WHERE follower = '" + follower.toLowerCase() + "'; failed with exception: " + e.getMessage());
		}
	}
	
	private ResultSet selectFollow(String follower) {
		System.out.println("Attempting query: SELECT * FROM follows WHERE follower='" + follower.toLowerCase() + "';");
		ResultSet result = null;
		try {
			result = session.execute("SELECT * FROM follows WHERE follower='" + follower.toLowerCase() + "';");
		} catch (Exception e){
			System.out.println("SELECT " + follower.toLowerCase() + " FROM follows threw and error: " + e.getMessage());
		}
		
		return result;
	}
	
	// keeping for support of insertFollowList(List<Follow followsList)
	public int insertFollow(String follower, String channel){
		System.out.println("Query in insertFollow(follower,channel): " + getInsertFollowStatement(follower.toLowerCase(),channel.toLowerCase()));
		try {
			session.execute(getInsertFollowStatement(follower.toLowerCase(),channel.toLowerCase()));
		} catch (Exception e) {
			System.out.println("INSERT INTO follows threw and error: " + e.getMessage());
		}
		
		return 0;
	}
	
	//this one is inefficient and only keeping incase somethingbreaks
	public int insertFollowList(List<Follow> followsList){
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
		statement = "INSERT INTO follows (uuid,follower,channel) VALUES (" + UUID.randomUUID() + ", '" + follower.toLowerCase() + "', '" + channel.toLowerCase() + "')";
		System.out.println("Generated statement: " + statement);
		return statement;
	}
	//optimal way to insert since batch inserts don't work righti n cassandra
	public static int threadedInsertFollowList(List<Follow> followsList){
		Iterator<Follow> followsListIterator = followsList.iterator();
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
	public void batchInsert(List<Follow> followsList){
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
