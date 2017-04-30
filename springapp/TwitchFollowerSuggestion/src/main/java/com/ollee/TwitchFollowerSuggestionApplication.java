package com.ollee;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import me.philippheuer.twitch4j.TwitchClient;
import me.philippheuer.twitch4j.model.Follow;



@SpringBootApplication
public class TwitchFollowerSuggestionApplication {


	public static void main(String[] args) {
		SpringApplication.run(TwitchFollowerSuggestionApplication.class, args);
//		CassandraDriver.initializeCassandra();
		
		CassandraDriver3.initializeCassandra();
		
//		List<Follow> temp = TwitchWrapper.getUserChannelsFollowed("ollee64");
//		Iterator<Follow> iter = temp.iterator();
//		while (iter.hasNext()){
//			System.out.println(iter.next().getChannel().getName().toLowerCase());
//		}
//		System.out.println("temp size: " + temp.size());
//		CassandraDriver2.insertFollowList("ollee64", temp);
		
//		Iterator<String> iter2 = CassandraDriver2.getFollowList("ollee64").iterator();
//		while(iter2.hasNext()){
//			System.out.println(iter2.next());
//		}
		
//		if(CassandraDriver2.checkIfUserChannelsFollowedAlreadyFetched("ollee64")){
//			System.out.println("ollee64 is in followers table");
//		} else{System.out.println("fail");
//}
//	
//		List<String> testList = new ArrayList<String>();
//		testList.add("blah");
//		CassandraDriver2.insertIntoChannels("test", testList);
////
//		for(int i = 0; i < 10; i++){
//			testList.add("TestUser000" + i);
//		}
//		for(int i = 10; i < 100; i++){
//			testList.add("TestUser00" + i);
//		}
//		for(int i = 100; i < 900; i++){
//			testList.add("TestUser0" + i);
//		}
//		
//		System.out.println("Test String: " + CassandraDriver2.jsonStringListString(testList));
//		System.out.println("Inserting");
//		CassandraDriver2.insertChannelFollowerList("testchannel", testList, true);
	
	}
}
