package com.ollee;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import me.philippheuer.twitch4j.model.Channel;
import me.philippheuer.twitch4j.model.Follow;

public final class TwitchAPICallHandler {
	
	private static List<Follow> userFollowsToInsertIntoDatabase = new LinkedList<Follow>();
	private static List<Follow> channelFollowers = new LinkedList<Follow>();
	private static TwitchAPIRateLimiter runMe = new TwitchAPIRateLimiter();

	public TwitchAPICallHandler(){
	}
	
	public static List<Channel> fetchChannelSuggestions(String username){
//level1
	//fetch channels user follows
		List<Follow> userFollows = null;
		System.out.println("TwitchAPICallHandler: Fetching channels that " + username + " follows.");
		if(!CassandraDriver.checkIfUserChannelsFollowedAlreadyFetched(username)){
			userFollows = TwitchWrapper.getUserChannelsFollowed(username);
			System.out.println("TwitchAPICallHandler: userfollows size(): " + userFollows.size());
		//fetch list channels a user follows already in cassandra database
			System.out.println("TwitchAPICallHandler: Fetching channels that " + username + " already follows in my database");
		//scrub userFollows of channels already in database
			userFollowsToInsertIntoDatabase = convertResultListToCleanFollowList(new LinkedList<Follow>(userFollows), CassandraDriver.selectFollow(username));
			System.out.println("TwitchAPICallHandler: userFollowsToInsertIntoDatabase.size()" + userFollowsToInsertIntoDatabase.size());
		//insert remaining users follows
			System.out.println("TwitchAPICallHandler: Cassandra threaded insert. userFollowsToInsertIntoDatabse.size(): " + userFollowsToInsertIntoDatabase.size());
			if(userFollowsToInsertIntoDatabase.size() > 0){
				System.out.println("TwitchAPICallHandler: Inserting userFollows into database");
				CassandraDriver.threadedInsertFollowList(userFollowsToInsertIntoDatabase);
				CassandraDriver.insertUserIntoAlreadyFetchedTable(username);
			}
		} else{
			userFollows = CassandraDriver.selectFollow(username);
			System.out.println("TwitchAPICallHandler: user was already in database");
		}
//level2
	//fetch followers of each of the channels in userFollows

	//this is where fresh code starts

		
		//check my database if the channels in userFollows are cached
		List<Follow> userFollowsToInsertIntoCassandra = removeChannelsAlreadyInDatabase(new LinkedList<Follow>(userFollows));
		System.out.println("TwitchAPICallHandler: userFollows: " + userFollows.size() + " to be inserted: " + userFollowsToInsertIntoCassandra.size());
		//enqueue the fetch with TwithAPIRateLimiter.enqueueFetchFromTwitchAndInsertIntoCassandra(channelName)
			//this fetches the followers of each channel not already in databse
		
		TwitchAPIRateLimiter.enqueueListToFetchFromTwitchAndInsertIntoCassandra(userFollowsToInsertIntoCassandra.subList(0, 1));
		if (!TwitchAPIRateLimiter.isStarted()){
			runMe.start();
		}
//level3
	//TODO fetch channels follows by mutual followers of channels user at level1 follows

		
		
	//this is where fresh code ends		

//		System.out.println("Anything after this is probably broken");
		
//		System.out.println("TwitchAPICallHandler: listOfThreads.size()" + TwitchAPIRateLimiter.getNumberOfThreads() + " and it should be: " + userFollows.size() );
//		int tempI = 0;
//		while(!TwitchAPIRateLimiter.isEmpty() && tempI < 50){
//			System.out.println("TwitchAPICallHandler: TwitchAPIRateLimiter.getFinishedsize(): " + TwitchAPIRateLimiter.getFinishedSize() + " RUN#: " + tempI++);
//			if(TwitchAPIRateLimiter.getFinishedSize() > 0){
//				System.out.println("TwitchAPICallHandler: getFinishedSize() was > 0, fetching ThreadedTwitchWrapperGetUserChannelsFollowed object"); 
//				ThreadedTwitchWrapperGetUserChannelsFollowed finishedQuery = TwitchAPIRateLimiter.getFinished();
//				System.out.println("TwitchAPICallHandler: fetching list from finishedQuery");
//				List<Follow> channelFollows = finishedQuery.getUserChannelsFollowedList();
//				System.out.println("TwitchAPICallHandler: channelFollows.size(): " + channelFollows.size() + " Attempting to threadedinsert");
//				CassandraDriver.threadedInsertFollowList(channelFollows);
//				tempI++;
//			}
//			//System.out.println("TwitchAPICallHandler: Hung up waiting threads to finish");
//			System.out.println("TwitchAPICallHandler: There are currently threads n = " + TwitchAPIRateLimiter.getNumberOfThreads());
//			try {//wait for threads to end
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
		
		return null;
	}

	private static List<Follow> removeChannelsAlreadyInDatabase(LinkedList<Follow> userFollows) {
		//userFollows a destroyable copy
		Follow next = null;
		Iterator<Follow> iterator = userFollows.iterator();
		while (iterator.hasNext()){
			next = iterator.next();
			if(CassandraDriver.checkIfChannelFollowersAlreadyFetched(next.getChannel().getName().toLowerCase())){
				System.out.println("removechannelsalreadyindatabse: " + next.getChannel().getName() + " was in the database");
				userFollows.remove(next);
			}
		}
		
		return userFollows;
	}


	private static List<Follow> convertResultListToCleanFollowList(List<Follow> userFollows, List<Follow> existingFollows) {
		//TODO make this deal with unfollows but for now lets go for functionalish
		Iterator<Follow> iteratorToString = existingFollows.iterator();
		List<String> stringListToRemove = new LinkedList<String>();
		while(iteratorToString.hasNext()){
			stringListToRemove.add(iteratorToString.next().getChannel().getName().toLowerCase());
		}
		Iterator<Follow> iter = userFollows.iterator();
		Follow workingFollow = null;
		while(iter.hasNext()){
			workingFollow=iter.next();
			if(stringListToRemove.contains(workingFollow.getChannel().getName().toLowerCase())){
				userFollows.remove(workingFollow);
			}
		}
		
		return userFollows;
	}

	private static List<String> convertUserFollowsToStringList(List<Follow> userFollows) {
		Iterator<Follow> iterator1 = userFollows.iterator();
		List<String> workingStringList = new LinkedList<String>();
		//int temporaryCounter = 0;
		System.out.println("TwitchAPICallHandler: Copy string values to userFollowsString");
		while(iterator1.hasNext()){//copy string values to userFollowsString
			workingStringList.add(iterator1.next().getChannel().getName());
			//System.out.println("TemporaryCounterhas counted: " + ++temporaryCounter + " times");
		}
		return workingStringList;
	}
	
}
