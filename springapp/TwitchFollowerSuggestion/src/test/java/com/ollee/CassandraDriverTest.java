package com.ollee;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CassandraDriverTest {
	
	@Test
	public void testInsertChannelAlreadyFetched() {
		CassandraDriver.initializeCassandra();
		String channelName = "testusername";
		
		assertEquals(false, CassandraDriver.checkIfChannelFollowersAlreadyFetched(channelName));
		
		CassandraDriver.insertChannelIntoAlreadyFetchedTable(channelName);
		
		assertEquals(true, CassandraDriver.checkIfChannelFollowersAlreadyFetched(channelName));
		
		CassandraDriver.deleteChannelFromAlreadyFetchedtable(channelName);
	}
	
	@Test
	public void testInsertUserAlreadyFetched(){
		CassandraDriver.initializeCassandra();
		
		String username = "testusername";
		
		assertEquals(false, CassandraDriver.checkIfUserChannelsFollowedAlreadyFetched(username));
		
		CassandraDriver.insertUserIntoAlreadyFetchedTable(username);
		
		assertEquals(true, CassandraDriver.checkIfUserChannelsFollowedAlreadyFetched(username));
		
		CassandraDriver.deleteUserFromAlreadyFetchedTable(username);
	}
}


/*


CassandraDriver.insertChannelIntoAlreadyFetchedTable(channelName);
CassandraDriver.insertUserIntoAlreadyFetchedTable(username);

CassandraDriver.checkIfChannelFollowersAlreadyFetched(channelName);
CassandraDriver.checkIfUserChannelsFollowedAlreadyFetched(username);

*/