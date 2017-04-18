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
		System.out.println("CassandraDriver: Building Cluster");
		cluster  = Cluster.builder().addContactPoint(cassandraServer).build();
		System.out.println("CassandraDriver: connection to cluster");
		session = cluster.connect(keyspaceName);
		System.out.println("CassandraDriver: creating table if doesn't exist");
		try {
			session.execute("CREATE TABLE IF NOT EXISTS twitchsuggestion.follows(follower text PRIMARY KEY, channel text);");
		} catch (Exception e) {
			System.out.println("CassandraDriver: CREATE TABLE threw an error: " + e.getMessage());
		}
	}

	private static void deleteRow(String follower) {
		System.out.print("CassandraDriver: Attempting query:  DELETE FROM follows WHERE follower= '" + follower.toLowerCase() + "';");
		try{
			session.execute("DELETE FROM follows WHERE follower= '" + follower.toLowerCase() + "';");
		} catch (Exception e){
			System.out.println("CassandraDriver: DELETE FROM follows WHERE follower = '" + follower.toLowerCase() + "'; failed with exception: " + e.getMessage());
		}
	}
	
	public static ResultSet selectFollow(String follower) {
		System.out.println("CassandraDriver: Attempting query: SELECT * FROM follows WHERE follower='" + follower.toLowerCase() + "';");
		ResultSet result = null;
		try {
			result = session.execute("SELECT * FROM follows WHERE follower='" + follower.toLowerCase() + "' ALLOW FILTERING;");
		} catch (Exception e){
			System.out.println("CassandraDriver: SELECT " + follower.toLowerCase() + " FROM follows threw and error: " + e.getMessage());
		}
		return result;
	}
	
	private static ResultSet selectFollowerAndChannel(String follower, String channel){
		ResultSet result = null;
		System.out.println("CassandraDriver: Trying to find follow event of follower: " + follower + " and channel: " + channel);
		try {
			result = session.execute("SELECT * FROM follows WHERE follower='" + follower.toLowerCase() + "' AND channel='" + channel.toLowerCase() + "' ALLOW FILTERING;");
		} catch(Exception e){
			System.out.println("CassandraDriver: SELECT follower: " + follower.toLowerCase() + " and channel: " + channel.toLowerCase() + " from follows threw error: " + e.getMessage());
		}
		return result;
	}
	
	// keeping for support of insertFollowList(List<Follow followsList)
	public static int insertFollow(String follower, String channel){
		System.out.println("CassandraDriver: Query in insertFollow(follower,channel,date,timestamp): " + getInsertFollowStatement(follower.toLowerCase(),channel.toLowerCase()));
		try {
			session.execute(getInsertFollowStatement(follower.toLowerCase(),channel.toLowerCase()));
		} catch (Exception e) {
			System.out.println("CassandraDriver: INSERT INTO follows threw and error: " + e.getMessage());
		}
		
		return 0;
	}
	
	//this one is inefficient and only keeping incase somethingbreaks
	public static int insertFollowList(List<Follow> followsList){
		Iterator<Follow> followsListIterator = followsList.iterator();
		while(followsListIterator.hasNext()){
			Follow follow = followsListIterator.next();
			System.out.println("CassandraDriver: running insertFollow for: " + follow.getUser().getName().toLowerCase() + " for channel: " + follow.getChannel().getName().toLowerCase());
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
		System.out.println("CassandraDriver: followsList.size(): " + followsList.size());
		List<String> duplicates = new LinkedList<String>();
		ResultSet results = selectFollow(followsList.get(0).getUser().getName().toLowerCase());
		List<Row> resultSetList = new LinkedList<Row>(results.all());
		System.out.println("CassandraDriver: Size of result set: " + resultSetList.size());
		if(resultSetList.size() != 0){
			Iterator<Row> resultSetIterator = resultSetList.iterator();
			while(resultSetIterator.hasNext()){
				String s = resultSetIterator.next().getString("channel");
				//System.out.println("CassandraDriver: candidate duplicate adding to list: " + s);
				if(s != null){
					duplicates.add(s.toLowerCase());
				}
			}
			System.out.println("CassandraDriver: Duplicates is size: " + duplicates.size());
			//remove duplicates from followsList
			//get list of strings of follows
			List<Follow> followsToRemove = new LinkedList<Follow>();
			while(followsListIterator.hasNext()){
				Follow next = followsListIterator.next();
				System.out.println("CassandraDriver: Checking if duplicates contains next");
				if(duplicates != null){
					System.out.println("CassandraDriver: Duplicates is not null");
					if(next.getChannel().getName().toLowerCase() != null){
						System.out.println("CassandraDriver: next channel name is not null");
						if(duplicates.contains(next.getChannel().getName().toLowerCase())){
							System.out.println("CassandraDriver: duplicates contains next");
							followsToRemove.add(next);
						}
					}
				}
			}
			
			followsList.removeAll(new HashSet<Follow>(followsToRemove));
		}

		followsListIterator = followsList.iterator();
		while(followsListIterator.hasNext()){
			Follow follow = followsListIterator.next();
			if (follow.getChannel().getName().toLowerCase() != null && follow.getUser().getName().toLowerCase() != null){
				System.out.println("CassandraDriver: running insertFollow for: " + follow.getUser().getName().toLowerCase() + " for channel: " + follow.getChannel().getName().toLowerCase());
				ThreadedInsert thread = new ThreadedInsert(
						getInsertFollowStatement(
								follow.getUser().getName().toLowerCase(),
								follow.getChannel().getName().toLowerCase()));
				thread.start();
			}
		}		
		return 0;
	}

	public static boolean checkIfChannelFollowersAlreadyFetched(String channelName){
		System.out.println("CassandraDriver: Attempting query: SELECT * FROM channelfollowersfetched WHERE channel='" + channelName.toLowerCase() + "';");
		ResultSet result = null;
		try { //query from table
			result = session.execute("SELECT * FROM channelfollowersfetched WHERE follower='" + channelName.toLowerCase() + "' ALLOW FILTERING;");
		} catch (Exception e){
			System.out.println("CassandraDriver: SELECT " + channelName.toLowerCase() + " FROM channelfollowersfetched threw and error: " + e.getMessage());
		}
		if(result == null){//check if there even is a result
			return false;
		}else  {
			if (result.all().get(1) != null){//if the result has stomething in it
				if(result.all().get(1).getString("channel").toLowerCase() == channelName.toLowerCase()){//if the something in it is matches channel name
					return true;//return true if it matches in the table
				}
			}	else{
				return false;//return false if it is not in the table
			}
		}
		return false;
	}
	
	public static void insertChannelIntoAlreadyFetchedTable(String channelName){
		if(checkIfChannelFollowersAlreadyFetched(channelName)){
			System.out.println("Inserting " + channelName + " into already checked caching table");
			session.execute("INSERT INTO channelfollowersfetched (channel, timestamp) VALUES (" + channelName.toLowerCase() + "','" + Date.from(Instant.now()).getTime() + "')");
			return;
		}
		System.out.println("Channel " + channelName + " already in already checked cache table");
	}
	
	public static boolean checkIfUserChannelsFollowedAlreadyFetched(String username){
		System.out.println("CassandraDriver: userchannelsfollowedfetched: Attempting query: SELECT * FROM userchannelsfollowedfetched WHERE user='" + username.toLowerCase() + "';");
		ResultSet result = null;
		try { //query from table
			result = session.execute("SELECT * FROM userchannelsfollowedfetched WHERE user='" + username.toLowerCase() + "';");
		} catch (Exception e){
			System.out.println("CassandraDriver: userchannelsfollowedfetched: SELECT " + username.toLowerCase() + " FROM userchannelsfollowedfetched threw and error: " + e.getMessage());
		}
		if(result == null){ //check if there even is a result
			System.out.println("CassandraDriver: userchannelsfollowedfetched: there was no resultset");
			return false;
		} else {
			System.out.println("CassandraDriver: userchannelsfollowedfetched: There was a resultset");
			if(result.all().get(1) != null){ //if there even is something
				System.out.println("There is a thing in the result set: " + result.all().get(1).getString("user"));
				if(result.all().get(1).getString("user").toLowerCase() == username.toLowerCase()){
					System.out.println("CassandraDriver: userchannelsfollowedfetched:  the names match: " + username.toLowerCase());
					return true; //return true if it matches the table username
				}
			} else{
				System.out.println("CassandraDriver: userchannelsfollowedfetched: username not in the table");
				return false; //if its not in the table
			}
		}
		return false;
	}
	
	public static void insertUserIntoAlreadyFetchedTable(String username){
		System.out.println("CassandraDriver: userchannelsfollowedfetched method entered");
		if(checkIfUserChannelsFollowedAlreadyFetched(username)){
			System.out.println("CassandraDriver: userchannelsfollowedfetched: Inserting " + username + " into already checked caching table");
			session.execute("INSERT INTO userchannelsfollowedfetched (user, timestamp) VALUES (" + username.toLowerCase() + "','" + Date.from(Instant.now()).getTime() + "')");
		}
		System.out.println("CassandraDriver: userchannelsfollowedfetched: User " + username + " already in already checked cache table");
	}

}
