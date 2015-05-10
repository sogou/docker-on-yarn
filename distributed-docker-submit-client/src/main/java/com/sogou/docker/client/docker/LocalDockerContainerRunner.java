package com.sogou.docker.client.docker;

import java.io.IOException;
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
  private final YarnDockerClient client;
  private float progress;
  private volatile boolean done = false;

  private YarnDockerClientParam yarnDockerClientParam;
  private int exitStatus = -1;

  public boolean isFinshed(){
    return done;
  }

  public int getExitStatus(){
    return exitStatus;
  }

  public void ensureContainerRemoved() {

  }

  public LocalDockerContainerRunner(YarnDockerClientParam param) {

    yarnDockerClientParam = param;
    client = new YarnDockerClient(yarnDockerClientParam);
  }


  public void run() throws IOException {

    exitStatus = client.runtask();
    done = true;
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
