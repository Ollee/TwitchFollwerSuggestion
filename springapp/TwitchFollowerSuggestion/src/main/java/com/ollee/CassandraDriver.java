package com.ollee;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

import me.philippheuer.twitch4j.model.Follow;

public class CassandraDriver {
	
	private static Cluster cluster;
	private Session session;
	private static String cassandraServer = "linode.ollee.net";
	private static String clusterName = "Test cluster";
	private static String keyspaceName = "twitchsuggestion";
	//private static Keyspace keyspace = null;
	
	/*ColumnFamily<String, String> CF_USER_INFO =
			new ColumnFamily<String,String>(
					"follows", 				//columnfamilyname
					StringSerializer.get(),		//key serializer
					StringSerializer.get());	//column serializer*/
	
	public CassandraDriver() {
		this.initializeCassandra();
	}
	
	private void initializeCassandra(){
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

	public int insertFollow(String follower, String channel){
		System.out.println("Query in insertFollow(follower,channel): " + getInsertFollowStatement(follower.toLowerCase(),channel.toLowerCase()));
		try {
			session.execute(getInsertFollowStatement(follower.toLowerCase(),channel.toLowerCase()));
		} catch (Exception e) {
			System.out.println("INSERT INTO follows threw and error: " + e.getMessage());
		}
		
		return 0;
	}
	
	public String getInsertFollowStatement(String follower, String channel){
		String statement = "";
		statement = "INSERT INTO follows (follower,channel) VALUES ('" + follower.toLowerCase() + "', '" + channel.toLowerCase() + "')";
		System.out.println("Generated statement: " + statement);
		return statement;
	}
	
	public int insertFollowList(List<Follow> followsList){
		Iterator<Follow> followsListIterator = followsList.iterator();
		while(followsListIterator.hasNext()){
			Follow follow = followsListIterator.next();
			System.out.println("running insertFollow for: " + follow.getUser().getName().toLowerCase() + " for channel: " + follow.getChannel().getName().toLowerCase());
			insertFollow(follow.getUser().getName().toLowerCase(), follow.getChannel().getName().toLowerCase());
		}		
		return 0;
	}
	
	public void batchInsert(List<Follow> followsList){
		Iterator<Follow> i = followsList.iterator();
		String batch = "BEGIN BATCH";
		while(i.hasNext()){
			Follow f = i.next();
			batch.concat(getInsertFollowStatement(f.getUser().getName().toLowerCase(),f.getChannel().getName().toLowerCase()) + "\n");
		}
		batch.concat("APPLY BATCH;");
		
		try{
			session.execute(batch);
		} catch (Exception e){
			System.out.println("Exception cauth in batchInsert: " + e.getMessage());
			System.out.println(e.getStackTrace().toString());
		}
	}

	
	
	
	// TODO batch insert
	// TODO automate list insert
	
	
	
	
	
	
	/*
	private static void initializeAstyanax(String address){
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
				.forCluster(clusterName)
				.forKeyspace(keyspaceName)
				.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
						.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
				)
				.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
						.setPort(9160)
						.setMaxConnsPerHost(1)
						.setSeeds("127.0.0.1:9160")
				)
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		keyspace = context.getClient();
	}
	
	//return 1 for success return 0 for fail
	public int addFollow(String user, String channel){
		MutationBatch m = keyspace.prepareMutationBatch();
		
		m.withRow(CF_USER_INFO, user)
			.putColumn("channel", channel);
		
		try {
			OperationResult<Void> result = m.execute();
		} catch (ConnectionException e){
			System.out.println(e.getMessage());
			return 0;
		}
		return 1;
	}*/
}
