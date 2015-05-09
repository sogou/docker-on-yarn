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

  public LocalDockerContainerRunner() {

    yarnDockerClientParam = buildYarnDockerClientParam();
    client = new YarnDockerClient(yarnDockerClientParam);
  }

  private YarnDockerClientParam buildYarnDockerClientParam() {
    YarnDockerClientParam p = new YarnDockerClientParam();

    // TODO Get those param from yarn
    p.cmdAndArgs = "echo Hello from docker on yarn".split("\\s+");
    p.containerMemory = 512;
    p.containerVirtualCores = 1;
    p.runnerScriptPath = "/Users/guoshiwei/DEV/docker-client/runner.py";
    p.dockerCertPath = "/Users/guoshiwei/.boot2docker/certs/boot2docker-vm";
    p.dockerHost = "192.168.59.103:2376";
    p.dockerImage = "registry.docker.dev.sogou-inc.com:5000/clouddev/sogou-rhel-base:6.5";
    return p;
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
