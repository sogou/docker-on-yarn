package com.sogou.docker.client.docker;

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
