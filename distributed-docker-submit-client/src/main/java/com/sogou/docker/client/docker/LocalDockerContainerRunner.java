package com.sogou.docker.client.docker;

import java.util.HashMap;
import java.util.Map;

/**
 * Create and run a container in the local docker service.
 *
 * Created by guoshiwei on 15/4/25.
 *
 * TODO: Implement LocalDockerContainerRunner
 */
public class LocalDockerContainerRunner {
  private float progress;
  private volatile boolean done = false;

  public static class DockerOptions {
    public String imageName ;
    public int containerCpuShares;
    public int containerMemoryLimit; // In bytes
    public String command;

    public String containerDataDir; // If given, `localDataDir` will be mapped into
                                        // `containerDataDir` of docker container.
    public String localDataDir;
    public String workingDir;

    public String runnerScriptPath; // The absolute path of runner script on host local filesystem

    // Env variables to be setup for the shell command
    public Map<String, String> cmdEnv = new HashMap<String, String>();
  }

  public boolean isFinshed(){
    return done;
  }

  public int getExitStatus(){
    return 0;
  }

  public void ensureContainerRemoved() {

  }

  public void run() {

    System.err.println("Hello from docker on yarn");
    progress = 100;
  }

  public float getProgress() {
    // TODO: Maybe out docker image can support get progress scripts
    return progress;
  }

  public void stop() {

    // TODO Stop and remove docker container instance.

  }
}
