package com.sogou.dockeronyarn.appmaster.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.DockerException;
import com.github.dockerjava.api.NotModifiedException;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class YarnDockerClient {

  private static final Log LOG = LogFactory.getLog(YarnDockerClient.class);
  private static String CONTAINER_RUNNER_SCRIPT_PATH = "/runner.py";
  private static String[] RUN_CMD = new String[]{"/usr/bin/python", CONTAINER_RUNNER_SCRIPT_PATH};

  private final YarnDockerClientParam yarnDockerClientParam;
  private long streamTimeout = 10 * 1000;
  private int stopTimeout = 60;

  private final DockerClient docker;

  private Thread stdoutThread;
  private Thread stderrThread;
  private Thread waitThread;

  private String containerId;
  private int exitcode = ExitCode.TIMEOUT.getValue();
  private volatile boolean containerStoped = false;
  private List<Bind> volumeBinds = new ArrayList<Bind>();


  public YarnDockerClient(YarnDockerClientParam yarnDockerClientParam) {
    this.yarnDockerClientParam = yarnDockerClientParam;
    this.docker = getDockerClient();
  }

  /**
   * Start container, non block.
   *
   * @throws IOException
   * @throws DockerException
   */
  public void startContainer() throws IOException, DockerException {
    LOG.info("Pulling docker image: " + yarnDockerClientParam.dockerImage);
    try {
      docker.pullImageCmd(yarnDockerClientParam.dockerImage).exec().close();
    }catch (IOException e){
      throw new RuntimeException("Pull docker image failed.", e);
    }

    CreateContainerCmd createContainerCmd = getCreateContainerCmd();
    LOG.info("Creating docker container: " + createContainerCmd.toString());
    this.containerId = createContainerCmd.exec().getId();

    LOG.info("Start docker container: " + containerId);
    getStartContainerCmd().exec();

    startLogTailingThreads(containerId);

    this.waitThread = new Thread(new Runnable() {
      @Override
      public void run() {
        WaitContainerCmd wc = docker.waitContainerCmd(containerId);
        try {
          exitcode = wc.exec();
          LOG.info(String.format("Container %s exited with exitCode=%d", containerId, exitcode));
        } catch (NotFoundException e) {
          LOG.error(String.format("Container %s not found", containerId), e);
          exitcode = ExitCode.CONTAINER_NOT_CREATE.getValue();
        }
      }
    }, "waitThread-" + containerId);

    waitThread.start();
  }

  /**
   * Block until the docker container exit
   *
   * @return Exit code of the container.
   */

  public int waitContainerExit() {
    try {
      waitThread.join(this.yarnDockerClientParam.clientTimeout);
      containerStoped = true;
    } catch (InterruptedException e) {
      LOG.info("Interrupted when waiting container to exit");
    }

    return exitcode;
  }

  private int runTask() throws IOException, DockerException {
    try {
      startContainer();
      return waitContainerExit();
    }catch (Exception e){
      LOG.error("Failed To Start container: " + e.getMessage(), e);
      return ExitCode.CONTAINER_NOT_CREATE.getValue();
    }
    finally {
      shutdown();
    }
  }

  private DockerClient getDockerClient() {
    LOG.info("Initializing Docker Client");
    DockerClientConfig.DockerClientConfigBuilder configBuilder = DockerClientConfig
            .createDefaultConfigBuilder();
    configBuilder.withLoggingFilter(this.yarnDockerClientParam.debugFlag)
            .withUri("https://" + yarnDockerClientParam.dockerHost)
            .withDockerCertPath(yarnDockerClientParam.dockerCertPath);
    DockerClientConfig config = configBuilder.build();

    return DockerClientBuilder.getInstance(config)
            .build();
  }

  private CreateContainerCmd getCreateContainerCmd() {
    ArrayList<String> cmds = new ArrayList<String>();
    Collections.addAll(cmds, RUN_CMD);
    Collections.addAll(cmds, yarnDockerClientParam.cmdAndArgs);
    yarnDockerClientParam.cmdAndArgs = cmds.toArray(yarnDockerClientParam.cmdAndArgs);


    CreateContainerCmd con = docker.createContainerCmd(this.yarnDockerClientParam.dockerImage);
    con.withCpuShares(this.yarnDockerClientParam.containerVirtualCores);
    con.withMemoryLimit(new Long(this.yarnDockerClientParam.containerMemory * 1024 * 1024));
    con.withAttachStderr(true);
    con.withAttachStdin(false);
    con.withAttachStdout(true);
    con.withCmd(this.yarnDockerClientParam.cmdAndArgs);
    return con;
  }

  private StartContainerCmd getStartContainerCmd() {
    StartContainerCmd startCmd = docker.startContainerCmd(containerId);

    this.volumeBinds.add(new Bind(yarnDockerClientParam.runnerScriptPath,
            new Volume(CONTAINER_RUNNER_SCRIPT_PATH), AccessMode.ro));

    startCmd.withBinds(volumeBinds.toArray(new Bind[0]));
    return startCmd;
  }

  public void shutdown() throws IOException {
    LOG.info("Finishing");

    try {
      stderrThread.join(this.streamTimeout);
      stdoutThread.join(this.streamTimeout);
    } catch (InterruptedException e) {
      LOG.warn(e);
    }

    docker.removeContainerCmd(containerId).exec();
    LOG.info(String.format("Container %s removed.", containerId));

    this.docker.close();
    LOG.info("Docker client closed");
  }


  private void startLogTailingThreads(final String containerId) {
    this.stdoutThread = createTailingThread(containerId, true);
    this.stderrThread = createTailingThread(containerId, false);

    stdoutThread.start();
    stderrThread.start();
  }

  private Thread createTailingThread(final String containerId, final boolean isStdout) {
    return new Thread() {
      @Override
      public void run() {
        BufferedReader reader = null;

        try {
          LogContainerCmd logCmd = docker.logContainerCmd(containerId);
          logCmd.withFollowStream(true);

          logCmd.withStdErr(!isStdout);
          logCmd.withStdOut(isStdout);
          logCmd.withTimestamps(false);

          InputStream input = logCmd.exec();
          reader = new BufferedReader(new InputStreamReader(input));
          String line;
          PrintStream out = isStdout? System.out: System.err;
          while ((line = reader.readLine()) != null && !isInterrupted()) {
           out.println(line.trim());
          }

          LOG.info(String.format("Tailing %s of container %s stopped",
                  isStdout ? "STDOUT" : "STDERR",
                  containerId));
        } catch (Exception e) {
          LOG.error(e);
        } finally {
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException e) {
             LOG.error(e);
            }
          }

        }
      }
    };
  }

  public static void main(String[] args) {

    int result = -1;
    try {
      YarnDockerClientParam yarnDockerClientParam = new YarnDockerClientParam();
      try {
        yarnDockerClientParam.initFromCmdlineArgs(args);
        if (yarnDockerClientParam.isPrintHelp) {
          yarnDockerClientParam.printUsage();
          System.exit(ExitCode.SUCC.getValue());
        }
      } catch (IllegalArgumentException e) {
        System.err.println(e.getLocalizedMessage());
        yarnDockerClientParam.printUsage();
        System.exit(ExitCode.ILLEGAL_ARGUMENT.getValue());
      }

      YarnDockerClient client = new YarnDockerClient(yarnDockerClientParam);
      Runtime.getRuntime().addShutdownHook(new Thread(client.new ShutdownHook(), "shutdownWork"));

      result = client.runTask();

    } catch (Throwable t) {
      LOG.fatal("Error running CLient", t);
      System.exit(ExitCode.FAIL.getValue());
    }

    if (result == 0) {
      LOG.info("docker task completed successfully");
      System.exit(ExitCode.SUCC.getValue());
    }

    LOG.info("Application failed to complete successfully");
    LOG.info("client ends with value: " + result);
    System.exit(result);
  }

  public int getExitStatus() {
    return exitcode;
  }

  public void stop() throws com.github.dockerjava.api.NotFoundException, NotModifiedException{
    if (this.containerId != null && !this.containerStoped) {
      LOG.info(String.format("Stopping DockerContainer %s", containerId));
      StopContainerCmd stopContainerCmd = docker.stopContainerCmd(containerId);
      stopContainerCmd.withTimeout(stopTimeout);
      stopContainerCmd.exec();
      containerStoped = true;
      LOG.info(String.format("DockerContainer %s  stoped", containerId));
    }
  }

  public float getProgress() {
    // TODO Implement getProgress
    return 0;
  }

  public class ShutdownHook implements Runnable {

    public void run() {
      LOG.info("shutdownhook start");

      if (containerId != null && !containerStoped) {
        containerStoped = true;
        StopContainerCmd stopContainerCmd = docker.stopContainerCmd(containerId);
        stopContainerCmd.withTimeout(stopTimeout);
        try {
          stopContainerCmd.exec();
        } catch (Exception e) {

          LOG.info("docker container " + containerId
                  + " has been killed", e);
        }

        LOG.info("container  stoped by shutdownhook");
      }

      if (stdoutThread != null && stdoutThread.isAlive()) {
        stdoutThread.interrupt();
      }
      if (stderrThread != null && stderrThread.isAlive()) {
        stderrThread.interrupt();
      }
      if (waitThread != null && waitThread.isAlive()) {
        waitThread.interrupt();
      }
      try {
        if (docker != null)
          docker.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      LOG.info("shutdownhook end");
    }

  }
}
