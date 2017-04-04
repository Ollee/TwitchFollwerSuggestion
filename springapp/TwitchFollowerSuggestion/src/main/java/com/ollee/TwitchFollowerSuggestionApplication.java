package com.ollee;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;



@SpringBootApplication
public class TwitchFollowerSuggestionApplication {

	public static void main(String[] args) {
		SpringApplication.run(TwitchFollowerSuggestionApplication.class, args);
		CassandraDriver.initializeCassandra();
	}
	
	

}
