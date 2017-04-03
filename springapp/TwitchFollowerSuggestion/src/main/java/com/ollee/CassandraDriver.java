package com.ollee;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraDriver {
	
	private static String clusterName = "Test cluster";
	private static String keyspaceName = "twitchsuggestion";
	private static Keyspace keyspace = null;
	
	ColumnFamily<String, String> CF_USER_INFO =
			new ColumnFamily<String,String>(
					"follows", 				//columnfamilyname
					StringSerializer.get(),		//key serializer
					StringSerializer.get());	//column serializer
	
	public CassandraDriver(String address) {
		CassandraDriver.initializeAstyanax(address);
	}
	
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
			return 0;
		}
		return 1;
	}
}
