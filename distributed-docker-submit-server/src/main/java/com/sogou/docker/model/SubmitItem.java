package com.sogou.docker.model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SubmitItem {
	private String appname = "defaultApp";
	private String docker_image = null;
	private String workingdir = null;
	private String command = null;
	private long container_memory = 512;
	private long master_memory = 128;
	private long container_retry = 3;
	private int container_vcores = 1;
	private int priority = 10;
	private long timeout = 86400000;
	private String queue = "default";
	private String[] virualdirs = null;
	private boolean debug = false;


	public SubmitItem() {
		super();
	}

	public SubmitItem(String appname, String docker_image, String workingdir, String command, long container_memory, long master_memory, long container_retry, int container_vcores, int priority, 
			long timeout, String queue, String[] virualdirs, boolean debug) {
		super();
		this.appname = appname;
		this.docker_image = docker_image;
		this.workingdir = workingdir;
		this.command = command;
		this.container_memory = container_memory;
		this.master_memory = master_memory;
		this.container_retry = container_retry;
		this.container_vcores = container_vcores;
		this.priority = priority;
		this.timeout = timeout;
		this.queue = queue;
		this.virualdirs = virualdirs;
		this.debug = debug;
	}

	public String[] getVirualdirs() {
		return virualdirs;
	}

	public void setVirualdirs(String[] virualdirs) {
		this.virualdirs = virualdirs;
	}

	public String getAppname() {
		return appname;
	}

	public void setAppname(String appname) {
		this.appname = appname;
	}

	public String getDocker_image() {
		return docker_image;
	}

	public void setDocker_image(String docker_image) {
		this.docker_image = docker_image;
	}
	
	public String getWorkingdir() {
		return workingdir;
	}

	public void setWorkingdir(String workingdir) {
		this.workingdir = workingdir;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public long getContainer_memory() {
		return container_memory;
	}

	public void setContainer_memory(long container_memory) {
		this.container_memory = container_memory;
	}

	public long getMaster_memory() {
		return master_memory;
	}

	public void setMaster_memory(long master_memory) {
		this.master_memory = master_memory;
	}

	public long getContainer_retry() {
		return container_retry;
	}

	public void setContainer_retry(long container_retry) {
		this.container_retry = container_retry;
	}

	public int getContainer_vcores() {
		return container_vcores;
	}

	public void setContainer_vcores(int container_vcores) {
		this.container_vcores = container_vcores;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	
}
