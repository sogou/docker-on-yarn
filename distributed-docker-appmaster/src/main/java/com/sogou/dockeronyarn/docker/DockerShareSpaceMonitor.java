package com.sogou.dockeronyarn.docker;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.github.dockerjava.core.DockerClientBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.api.command.RemoveContainerCmd;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig.DockerClientConfigBuilder;


public class DockerShareSpaceMonitor {
	
	private static final Log LOG = LogFactory.getLog(DockerShareSpaceMonitor.class);
	
	public static long cLeanInterv = 86400000 * 3;
	
	public static void main(String[] args) {
		
		//CleanDockerShareSpaceRunner cdsr = new CleanDockerShareSpaceRunner();
		CleanDockerSpaceRunner cdsr = new CleanDockerSpaceRunner();
		Runtime.getRuntime().addShutdownHook(new Thread(new ShutDownHook(cdsr), "shutdownhook"));
		
		Thread thread = new Thread(cdsr, "dockerSpacecleaner");
		thread.start();
		
	}
	
	public static class ShutDownHook implements Runnable{
		public CleanDockerSpaceRunner cleanDockerShareSpaceRunner;
		
		public ShutDownHook(CleanDockerSpaceRunner cleanDockerShareSpaceRunner){
			this.cleanDockerShareSpaceRunner = cleanDockerShareSpaceRunner;
		}
		
		public void run(){
			cleanDockerShareSpaceRunner.stoped();
			while(!cleanDockerShareSpaceRunner.isKilled()){
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public static class CleanDockerShareSpaceRunner implements Runnable{


		volatile boolean stoped = false;
		volatile boolean killed = false;
		
		@Override
		public void run() {
			while(!stoped){
				File file = new File(Constants.DOCKER_USER_SPACE);
				if(!file.isDirectory()){
					LOG.error(Constants.DOCKER_USER_SPACE + "is not dir");
					try {
						Thread.sleep(1000L);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					continue;
				}
				File[] files = file.listFiles();
				if(files == null){
					try {
						Thread.sleep(1000L);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					continue;
				}
				for(int i = 0; i < files.length; ++i){
					if(this.stoped) continue;
					if(files[i] == null) continue;
					if(files[i].isFile()){
						if(!  files[i].delete()){
							LOG.error(files[i].getAbsolutePath() + "can not be deleted");
						}
					}else if(files[i].isDirectory()){
						long lastModified = files[i].lastModified();
						if(System.currentTimeMillis() - lastModified > cLeanInterv){
							deleteDir(files[i]);
						}
					}
				}
				if(this.stoped == false){
					try {
						Thread.sleep(10000L);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			killed = true;
			
		}
		
		private void deleteDir(File file) {
			if(this.stoped) return;
			if(file.isDirectory()){
				File[] files = file.listFiles();
				if(files == null){
					boolean del = file.delete();
					if(!del){
						LOG.error(file.getAbsolutePath() + "can not be deleted");
					}
					return;
				}
				for(int i = 0; i < files.length; ++i){
					if(files[i] == null) continue;
					deleteDir(files[i]);
				}
				if(this.stoped) return;
				boolean del = file.delete();
				if(!del){
					LOG.error(file.getAbsolutePath() + "can not be deleted");
				}
			}else{
				if(this.stoped) return;
				boolean del = file.delete();
				if(!del){
					LOG.error(file.getAbsolutePath() + "can not be deleted");
				}
			}
			
		}

		public void stoped(){
			this.stoped = true;
		}
		
		public boolean isKilled(){
			return this.killed;
		}
		
	}
	
	
	public static class CleanDockerSpaceRunner implements Runnable{


		volatile boolean stoped = false;
		volatile boolean killed = false;
		
		@Override
		public void run() {
			while(!stoped){
				
					DockerClientConfigBuilder configBuilder = DockerClientConfig
							.createDefaultConfigBuilder();
					DockerClientConfig config = configBuilder.build();

					 DockerClient docker = DockerClientBuilder.getInstance(config)
							.build();
					do{
						 try{
							 ListContainersCmd listContainerCmd = docker.listContainersCmd();
							 listContainerCmd.withShowAll(true);
							 List<Container> containers = listContainerCmd.exec();
							 
							
							 
							 if(containers == null || containers.size() == 0) break; 
							 
							 for(int i = 0; i < containers.size(); ++i){
								 
									 if(this.stoped) break;
									 Container container = containers.get(i);
									 if(container == null) continue;
									 
									 if(System.currentTimeMillis() - container.getCreated()* 1000 < cLeanInterv){
										 continue;
									 }
									 try{
										 removeContainer(docker, container);
									 }catch(Exception e){
										 e.printStackTrace();
									 }
									 
									 
									
								 }
								 
							 
							 
						 }catch(Exception e){
							 e.printStackTrace();
							 LOG.warn("cleanning docker meets exception " + e.getMessage() );
						 }
					} while(false);

				try {
					docker.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if(!this.stoped){
					try {
						Thread.sleep(10000L);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			killed = true;
			
		}
		
		

		private void removeContainer(DockerClient docker , Container container) {
			 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			 InspectContainerCmd inspectContainerCmd = docker.inspectContainerCmd(container.getId());
			 InspectContainerResponse response = inspectContainerCmd.exec();
			 if(response == null) return;
			 ContainerState state = response.getState();
			 if(state == null) return;
			 
			 if(state.isRunning()) return;
			 
			 
			 String finishTime = state.getFinishedAt();
			 finishTime = finishTime.replace('T', ' ').replace('Z', ' ');
			
			 Date date = null;
			try {
				date = sdf.parse(finishTime);
			} catch (java.text.ParseException e) {
				LOG.warn("date parse exception: "+ e.getMessage());
			}

			 if(date == null || System.currentTimeMillis() -  date.getTime() > cLeanInterv){
				 LOG.info("deleting container id: " + container.getId() + " finishTime: " + finishTime);
				 RemoveContainerCmd removeCmd = docker.removeContainerCmd(container.getId());
				 removeCmd.withRemoveVolumes(true);
				 removeCmd.withForce(true);
				 removeCmd.exec();
				 File file = new File(Constants.DOCKER_USER_SPACE + "/" + container.getId());
				 if(file != null && file.isDirectory()){
					 Process deleteProcess = null;
					try {
						deleteProcess = Runtime.getRuntime().exec("rm -rf " + Constants.DOCKER_USER_SPACE + "/" + container.getId());
						deleteProcess.waitFor();
						 LOG.warn("deleting " + Constants.DOCKER_USER_SPACE + "/" + container.getId() + " succ: " + (deleteProcess.exitValue() == 0) );
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					 
					
				 }
			 }
			
		}



		public void stoped(){
			this.stoped = true;
		}
		
		public boolean isKilled(){
			return this.killed;
		}
		
	}
}
