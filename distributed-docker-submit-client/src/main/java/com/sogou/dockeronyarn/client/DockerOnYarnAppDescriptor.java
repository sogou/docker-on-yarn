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

    private String workDir ="";

    public String getWorkDir() {
        return workDir;
    }

    public DockerOnYarnAppDescriptor setWorkDir(String workDir) {
        this.workDir = workDir;
        return this;
    }

    private String mountVolumne  ;

    public String getMountVolumne() {
        return mountVolumne;
    }

    public void setMountVolumne(String mountVolumne) {
        this.mountVolumne = mountVolumne;
    }

    public String getAmJarPath() {
        return amJarPath;
    }

    public DockerOnYarnAppDescriptor setAmJarPath(String amJarPath) {
        this.amJarPath = amJarPath;
        return this ;
    }

    private String commandToRun;
    private String amJarPath ;

  public DockerOnYarnAppDescriptor setAppName(String appName) {
    this.appName = appName;
    return this;
  }

  public DockerOnYarnAppDescriptor setAmPriority(int amPriority) {
    this.amPriority = amPriority; return this;
  }

  public DockerOnYarnAppDescriptor setAmQueue(String amQueue) {
    this.amQueue = amQueue; return this;
  }

  public DockerOnYarnAppDescriptor setContainer_retry(int container_retry) {
    this.container_retry = container_retry; return this;
  }

  public DockerOnYarnAppDescriptor setLog4jPropFile(String log4jPropFile) {
    this.log4jPropFile = log4jPropFile; return this;
  }

  public DockerOnYarnAppDescriptor setClientTimeout(long clientTimeout) {
    this.clientTimeout = clientTimeout; return this;
  }

  public DockerOnYarnAppDescriptor setDockerImage(String dockerImage) {
    this.dockerImage = dockerImage; return this;
  }

  public DockerOnYarnAppDescriptor setCommandToRun(String commandToRun) {
    this.commandToRun = commandToRun; return this;
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
