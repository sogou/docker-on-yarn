package com.sogou.dockeronyarn.client;

/**
 * Created by guoshiwei on 15/5/16.
 */
public class DockerOnYarnAppDescriptor {
  // Start time for client
  private final long clientStartTime = System.currentTimeMillis();
  // Application master specific info to register a new Application with RM/ASM
  private String appName = "";
  // App master priority
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "";

  // log4j.properties file
  // if available, add to local resources and set into classpath
  private String log4jPropFile = "";
  private long clientTimeout;
  private String dockerImage;
  private int container_retry = 3;
  private String commandToRun;

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public void setAmPriority(int amPriority) {
    this.amPriority = amPriority;
  }

  public void setAmQueue(String amQueue) {
    this.amQueue = amQueue;
  }

  public void setContainer_retry(int container_retry) {
    this.container_retry = container_retry;
  }

  public void setLog4jPropFile(String log4jPropFile) {
    this.log4jPropFile = log4jPropFile;
  }

  public void setClientTimeout(long clientTimeout) {
    this.clientTimeout = clientTimeout;
  }

  public void setDockerImage(String dockerImage) {
    this.dockerImage = dockerImage;
  }

  public void setCommandToRun(String commandToRun) {
    this.commandToRun = commandToRun;
  }

  public long getClientStartTime() {
    return clientStartTime;
  }

  public String getAppName() {
    return appName;
  }

  public int getAmPriority() {
    return amPriority;
  }

  public String getAmQueue() {
    return amQueue;
  }

  public String getLog4jPropFile() {
    return log4jPropFile;
  }

  public long getClientTimeout() {
    return clientTimeout;
  }

  public String getDockerImage() {
    return dockerImage;
  }

  public int getContainer_retry() {
    return container_retry;
  }

  public String getCommandToRun() {
    return commandToRun;
  }
}
