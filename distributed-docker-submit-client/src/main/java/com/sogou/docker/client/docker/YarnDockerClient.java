package com.sogou.docker.client.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.NotModifiedException;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class YarnDockerClient {


  private static final Log LOG = LogFactory.getLog(YarnDockerClient.class);
  private static String CONTAINER_RUNNER_SCRIPT_PATH = "/runner.py";

  private static String[] RUN_CMD = new String[]{"/usr/bin/python", CONTAINER_RUNNER_SCRIPT_PATH};
  public final YarnDockerClientParam yarnDockerClientParam = new YarnDockerClientParam();

  private long streamTimeout = 10 * 1000;
  private int stopTimeout = 60;

  private Thread stdoutThread;

  private Thread stderrThread;

  private WaitTaskRunner wtr;

  private Thread waitThread;

  private String containerId;

  private volatile boolean containerStoped = false;

  private DockerClient docker;

  private Process pullProcess = null;

  private int pullProcessTryNum = 3;

  private String runPath;

  private List<Bind> volumeBinds = new ArrayList<Bind>();

  public YarnDockerClient() throws Exception {

  }


  public boolean init(String[] args) throws ParseException {
    try{
      this.yarnDockerClientParam.initFromCmdlineArgs(args);
    }catch(Exception e){
      System.err.println(e.getMessage());
      yarnDockerClientParam.printUsage();
      return false;
    }
    ArrayList<String> cmds = new ArrayList<String>();
    Collections.addAll(cmds, RUN_CMD);
    Collections.addAll(cmds,yarnDockerClientParam.cmdAndArgs);
    yarnDockerClientParam.cmdAndArgs = cmds.toArray(yarnDockerClientParam.cmdAndArgs);

    this.volumeBinds.add(new Bind(yarnDockerClientParam.runnerScriptPath,
            new Volume(CONTAINER_RUNNER_SCRIPT_PATH), AccessMode.ro));

    return true;
  }

  public int runtask() throws IOException {
    DockerClientConfig.DockerClientConfigBuilder configBuilder = DockerClientConfig
            .createDefaultConfigBuilder();
    configBuilder.withLoggingFilter(this.yarnDockerClientParam.debugFlag);
    DockerClientConfig config = configBuilder.build();

    this.docker = DockerClientBuilder.getInstance(config)
            .build();

    boolean existed = assentPullImage();

    if (!existed) {
      return ExitCode.IMAGE_NOTFOUND.getValue();
    }


    CreateContainerCmd con = docker.createContainerCmd(this.yarnDockerClientParam.dockerImage);
    con.withCpuShares(this.yarnDockerClientParam.containerVirtualCores);
    con.withMemoryLimit(new Long(this.yarnDockerClientParam.containerMemory * 1024 * 1024));
    con.withAttachStderr(true);
    con.withAttachStdin(true);
    con.withAttachStdout(true);
    con.withCmd(this.yarnDockerClientParam.cmdAndArgs);
    final CreateContainerResponse response;
    try {
      response = con.exec();
    } catch (Exception e) {
      e.printStackTrace();
      return ExitCode.CONTAINER_NOT_CREATE.getValue();
    }
    this.containerId = response.getId();

    StartContainerCmd startCmd = docker.startContainerCmd(response.getId());
    startCmd.withBinds(volumeBinds.toArray(new Bind[0]));
//		bindHostDir(startCmd);

    try {
      startCmd.exec();
    } catch (NotFoundException e) {
      e.printStackTrace();
      return ExitCode.CONTAINER_NOT_CREATE.getValue();
    } catch (NotModifiedException e) {
      e.printStackTrace();
    }

    startLogTaillingThreads(response);

    this.wtr = new WaitTaskRunner(docker, response.getId());
    this.waitThread = new Thread(wtr, "waitThread");

    try {
      waitThread.start();
    } catch (IllegalThreadStateException e) {
    }
    int value;
    try {
      waitThread.join(this.yarnDockerClientParam.clientTimeout);
    } catch (InterruptedException e) {
      System.out.println("container  interrupted");
      e.printStackTrace();
    }

    value = wtr.getExitCode();

    if (this.containerId != null && !this.containerStoped) {
      containerStoped = true;
      StopContainerCmd stopContainerCmd = docker.stopContainerCmd(containerId);
      stopContainerCmd.withTimeout(stopTimeout);
      try {
        stopContainerCmd.exec();
      } catch (Exception e) {

        LOG.info("docker container " + response.getId()
                + " has been killed", e);
      }
      LOG.info("container  stoped by main");

    }

    finish();

    return value;
  }

  private void finish() {
    try {
      stderrThread.join(this.streamTimeout);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      stdoutThread.join(this.streamTimeout);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      this.docker.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void startLogTaillingThreads(final CreateContainerResponse response) {
    this.stdoutThread = new Thread() {
      @Override
      public void run() {
        BufferedReader reader = null;
        try {
          LogContainerCmd logCmd = docker.logContainerCmd(response
                  .getId());
          logCmd.withFollowStream(true);
          logCmd.withStdErr(false);
          logCmd.withStdOut(true);
          logCmd.withTimestamps(true);

          InputStream input = logCmd.exec();
          reader = new BufferedReader(new InputStreamReader(input));
          String line;
          while ((line = reader.readLine()) != null && !isInterrupted()) {
            System.out.println(line);
          }

        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          if (reader != null) {
            try {
              reader.close();
              System.out.println("stdout closed");
            } catch (IOException e) {
              e.printStackTrace();
            }
          }

        }
      }
    };

    this.stderrThread = new Thread() {

      @Override
      public void run() {
        BufferedReader reader = null;
        try {
          LogContainerCmd logCmd = docker.logContainerCmd(response
                  .getId());
          logCmd.withFollowStream(true);
          logCmd.withStdErr(true);
          logCmd.withStdOut(false);
          logCmd.withTimestamps(true);
          InputStream input = logCmd.exec();
          reader = new BufferedReader(new InputStreamReader(input));
          String line;
          while ((line = reader.readLine()) != null && !isInterrupted()) {

            System.err.println(line);

          }

        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          if (reader != null) {
            try {
              reader.close();
              System.out.println("stderr closed");
            } catch (IOException e) {
              e.printStackTrace();
            }
          }

        }
      }
    };

    try {
      stdoutThread.start();
    } catch (IllegalThreadStateException e) {
    }

    try {
      stderrThread.start();
    } catch (IllegalThreadStateException e) {
    }
  }

  private void bindHostDir(StartContainerCmd startCmd) {
    // TODO Auto-generated method stub
    Bind[] binds = null;
    if (this.yarnDockerClientParam.virtualDirs != null) {
      binds = new Bind[this.yarnDockerClientParam.virtualDirs.length + 1];
      File file = new File(Constants.DOCKER_USER_SPACE + "/" + this.containerId);
      boolean filesucc = false;
      try {
        filesucc = file.mkdir();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(
                "can not mkdir " + Constants.DOCKER_USER_SPACE + "/" + this.containerId + " exception: " + e.getMessage());
      }
      if (!filesucc) {
        throw new RuntimeException(
                "fail to  mkdir " + Constants.DOCKER_USER_SPACE + "/" + this.containerId);
      }


      try {
        for (int i = 0; i < yarnDockerClientParam.virtualDirs.length; ++i) {
          binds[i] = Bind.parse(Constants.DOCKER_USER_SPACE + "/" + this.containerId + ":" + this.yarnDockerClientParam.virtualDirs[i]);
        }


      } catch (Exception e) {
        e.printStackTrace();
        throw new IllegalArgumentException(
                " Illegal virtualdir specified for YarnDockerClient to run");
      }
    }

    if (binds == null) {
      binds = new Bind[1];
    }
    System.out.println("runpath: " + runPath);
    binds[binds.length - 1] = Bind.parse(runPath + ":rw");
    startCmd.withBinds(binds);
  }

  private boolean assentPullImage() throws IOException {
    PullImageCmd pullImageCmd = docker.pullImageCmd(yarnDockerClientParam.dockerImage);
    pullImageCmd.exec().close();
    return true;
  }

  public static void main(String[] args) {

    System.err.println(System.getProperty("java.class.path"));
    int result = -1;
    try {
      YarnDockerClient client = new YarnDockerClient();
      Runtime.getRuntime().addShutdownHook(new Thread(client.new ShutdownHook(), "shutdownWork"));
      LOG.info("Initializing Client");
      try {
        boolean doRun = client.init(args);
        if (!doRun) {
          System.exit(ExitCode.SUCC.getValue());
        }
      } catch (IllegalArgumentException e) {
        System.err.println(e.getLocalizedMessage());
        System.exit(ExitCode.ILLEGAL_ARGUMENT.getValue());
      } catch (RuntimeException e1) {
        System.err.println(e1.getLocalizedMessage());
        System.exit(ExitCode.RUNTIME_EXCEPTION.getValue());
      }
      result = client.runtask();
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

  public class WaitTaskRunner implements Runnable {
    private int exitcode = ExitCode.TIMEOUT.getValue();
    private DockerClient docker;
    private String id;

    public WaitTaskRunner(DockerClient docker, String id) {
      this.docker = docker;
      this.id = id;
    }

    @Override
    public void run() {
      WaitContainerCmd wc = docker.waitContainerCmd(id);
      try {
        exitcode = wc.exec();
        System.out.println("waitThread end");
      } catch (NotFoundException e) {
        e.printStackTrace();
      }

    }

    public int getExitCode() {
      return this.exitcode;
    }

    public void setExitCode(int value) {
      this.exitcode = value;
    }
  }

  public class ShutdownHook implements Runnable {

    public void run() {
      LOG.info("shutdownhook start");
      if (pullProcess != null) {
        pullProcess.destroy();
      }

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
