package com.ollee.deprecated;

public class ThreadedInsert implements Runnable {
	private Thread t;
	private String threadQuery;
	
	public ThreadedInsert(String query){
		threadQuery = query;
		//System.out.println("ThreadedInsert: Created thread for: " + threadQuery);
	}
	
	@Override
	public void run() {
		try {
			CassandraDriver.getSession().execute(threadQuery);
		} catch (Exception e){
			System.out.println("ThreadedInsert: Caught exception in thread: " 
						+ e.getMessage() 
						+ e.getStackTrace().toString());
		}
		
	}
	
	public void start(){
		//System.out.println("Starting thread for: " + threadQuery);
		if (t == null) {
			t = new Thread (this, threadQuery);
			t.start();
		}
	}
	
}
