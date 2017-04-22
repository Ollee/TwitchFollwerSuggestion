package com.ollee;

import java.util.Iterator;
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
		
		CassandraDriver2.initializeCassandra();
		
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
	
	
	}
}
