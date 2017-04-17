package com.ollee;

import java.time.Instant;
import java.util.Date;

import lombok.Getter;

public class ThreadedGetUserChannelsTimePair {
	@Getter
	long date;//miliseconds since epoch
	@Getter
	ThreadedTwitchWrapperGetUserChannelsFollowed t;
	
	public ThreadedGetUserChannelsTimePair(ThreadedTwitchWrapperGetUserChannelsFollowed thread){
		t = thread;
		date = Date.from(Instant.now()).getTime();
	}
}
