package com.sogou.docker.server;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.log4j.PropertyConfigurator;

import com.sogou.docker.util.LogUtil;



public class DockerSubmitMain {
	
	private static Object lock = new Object();

	public static void main(String[] args) throws Exception { 
		Thread.currentThread().setName("Main" + RandomUtils.nextInt());
		Runtime.getRuntime().addShutdownHook(new ShutdownHook());
		try {
			DockerServer server = new DockerServer();
			server.startHttpService();
			
			synchronized(lock){
				lock.wait();
			}
			
			
		} catch (Exception e) {
			System.exit(0);
		}
	}
	
	public static class ShutdownHook extends Thread{
		public void run(){
			try{
				lock.notifyAll();
			}catch(IllegalMonitorStateException e){
				
			}
			
		}
	}
	
	
}
