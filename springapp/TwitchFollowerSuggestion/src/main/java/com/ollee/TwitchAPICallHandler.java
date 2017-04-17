package com.ollee;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import me.philippheuer.twitch4j.model.Channel;
import me.philippheuer.twitch4j.model.Follow;

public final class TwitchAPICallHandler {
	
	private static List<String> userFollowsString = new LinkedList<String>();
	private static List<String> channelsFollowedNotInDatabase = new LinkedList<String>();
	private static List<Follow> userFollowsToInsertIntoDatabase = new LinkedList<Follow>();
	private static List<Follow> channelFollowers = new LinkedList<Follow>();
	private static List<ThreadedTwitchWrapperGetUserChannelsFollowed> listOfThreads = new LinkedList<ThreadedTwitchWrapperGetUserChannelsFollowed>();

	public TwitchAPICallHandler(){
	}
	
	public static List<Channel> fetchChannelSuggestions(String username){
		
		//fetch channels user follows
		System.out.println("Fetching channels that " + username + " follows.");
		List<Follow> userFollows = TwitchWrapper.getUserChannelsFollowed(username);//fethc channels user follows
		System.out.println("userfollows size(): " + userFollows.size());
		//turn userfollows into lsit of strings
		Iterator<Follow> iterator1 = userFollows.iterator();
		//int temporaryCounter = 0;
		System.out.println("Copy string values to userFollowsString");
		while(iterator1.hasNext()){//copy string values to userFollowsString
			userFollowsString.add(iterator1.next().getChannel().getName());
			//System.out.println("TemporaryCounterhas counted: " + ++temporaryCounter + " times");
		}
		System.out.println("userFollowsString.size(): " + userFollowsString.size());
		System.out.println("Fetching channels that " + username + " already follows in my database");
		ResultSet queryResults = CassandraDriver.selectFollow(username);//get user follwos already in databsae
		System.out.println("quesryResults.all().size()" + queryResults.all().size());
		Iterator<Row> iterator2 = queryResults.iterator();
		while(iterator2.hasNext()){//if username already in database, don't add to list
			String temp = iterator2.next().getString("channel");
			if(!userFollowsString.contains("temp")){
				channelsFollowedNotInDatabase.add(temp);
			}
		}
		System.out.println("follows not already in databse: " + channelsFollowedNotInDatabase.size());

		System.out.println("Translating channelsFollowedNotInDatabase from string to Follow");
		iterator1 = userFollows.iterator(); //refresh iterator
		while(iterator1.hasNext()){//this gives me a list of follows not alreayd in databse to uplaod
			Follow tempFollow = iterator1.next();
			if(channelsFollowedNotInDatabase.contains(tempFollow.getChannel().getName())){
				userFollowsToInsertIntoDatabase.add(tempFollow);
			}
		}
		System.out.println("userFollowsToInsertIntoDatabase.size()" + userFollowsToInsertIntoDatabase.size());
			//fetch follows older than 24 hours or that don't exist
			//insert new follows into channel
		System.out.println("Cassandra threaded insert");
		int tempDebugReturn = CassandraDriver.threadedInsertFollowList(userFollowsToInsertIntoDatabase);
		System.out.println("CassandraDriver.threadedInsertFollowList(userFollowsToInsertIntoDatabase) returned: " + tempDebugReturn);
		//fetch followers of that channel - this needs to be threaded and rate limited
			//api call to fetch all users that supply weight to channels to be followed
		//TODO make this check for cached items first...forreal IMPORTANT
		iterator1 = userFollows.iterator();
		System.out.println("Launching threads and adding them to lsitOfThreads");
		while(iterator1.hasNext()){
			ThreadedTwitchWrapperGetUserChannelsFollowed thread = new ThreadedTwitchWrapperGetUserChannelsFollowed(
					iterator1.next().getChannel().getName());
			listOfThreads.add(thread);
			TwitchAPIRateLimiter.addElement(thread);
			listOfThreads.get(listOfThreads.size() - 1).start();
		}
		System.out.println("listOfThreads.size()" + listOfThreads.size() + " and it should be: " + userFollows.size() );
		while(!TwitchAPIRateLimiter.isEmpty()){
			System.out.println("Hung up waiting threads to finish");
			System.out.println("There are currently threads n = " + TwitchAPIRateLimiter.getNumberOfThreads());
			try {//wait for threads to end
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}}
		//populate channelFollowers with all followers of channels user follows
		Iterator<ThreadedTwitchWrapperGetUserChannelsFollowed> listOfThreadsIterator = listOfThreads.iterator();
		while(listOfThreadsIterator.hasNext()){
			channelFollowers.addAll(listOfThreadsIterator.next().getUserChannelsFollowedList());
		}
		CassandraDriver.threadedInsertFollowList(channelFollowers);
		//fetch channels those followers follow
			//check if channels in database already
			//fetch follows older than 24 hours or that don't exist
			//insert new follows into channel
		
		//use now updated databse to generate suggestions based on weight
			//mutual following users follows provide +1
			//possibly check ignore list....
		
		
		return null;
	}
	
}
