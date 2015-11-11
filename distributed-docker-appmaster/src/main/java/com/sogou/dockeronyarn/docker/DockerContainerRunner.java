package com.sogou.dockeronyarn.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.DockerException;
import com.github.dockerjava.api.NotModifiedException;
import com.github.dockerjava.api.command.*;
import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DockerContainerRunner {

  private static final Log LOG = LogFactory.getLog(DockerContainerRunner.class);
  private static String CONTAINER_RUNNER_SCRIPT_PATH = "/runner.py";
  private static String[] RUN_CMD = new String[]{"/usr/bin/python", CONTAINER_RUNNER_SCRIPT_PATH};

  private final DockerContainerRunnerParam param;
  private int stopTimeout = 60;

  private final DockerClient docker;

  private Thread stdoutThread;
  private Thread stderrThread;
  private Thread waitThread;

  private String containerId;
  private int exitcode = ExitCode.TIMEOUT.getValue();
  private volatile boolean containerStopped = false;
  private volatile boolean isStopContainerRequested = false;
  private List<Bind> volumeBinds = new ArrayList<Bind>();

  private static final long DEFAULT_CONTAINER_MEMORY = 1 *1024 *1024 *1024L ;
  private static final int  DEFAULT_CONTAINER_CPU_SHARES = 512 ;


  public DockerContainerRunner(DockerContainerRunnerParam param) {
    this.param = param;
    this.docker = createDockerClient();

    Runtime.getRuntime().addShutdownHook(
            new Thread("shutdown DockerContainerRunner"){
              public void run(){
                LOG.info("shutdownhook start");
                try {
                  shutdown();
                } catch (IOException e) {
                  LOG.warn(e);
                }
                LOG.info("shutdownhook end");
              }
            }
    );
  }

  /**
   * Start container, non block.
   *
   * @throws IOException
   * @throws DockerException
   */
  public void startContainer(String containerName) throws IOException, DockerException {
    LOG.info("Pulling docker image: " + param.dockerImage);
    try {
      docker.pullImageCmd(param.dockerImage).exec().close();
    }catch (IOException e){
      throw new RuntimeException("Pull docker image failed.", e);
    }

    CreateContainerCmd createContainerCmd = getCreateContainerCmd(containerName);
    LOG.info("Creating docker container: " + createContainerCmd.toString());
    this.containerId = createContainerCmd.exec().getId();

    LOG.info("Start docker container: " + containerId);
    docker.startContainerCmd(containerId).exec();

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

    final long WAIT_INTERVAL = 100;
    long waitedMilliSecs = 0;

    while(true){
      if(isStopContainerRequested){
        doStopContainer("user requested");
      }

      if((param.clientTimeout > 0 ) && waitedMilliSecs >= param.clientTimeout){
        doStopContainer(String.format("Timeout for %d seconds", waitedMilliSecs/1000));
      }

      try {
        long waitStart = System.currentTimeMillis();
        waitThread.join(WAIT_INTERVAL);
        containerStopped = true;
        waitedMilliSecs += System.currentTimeMillis() - waitStart;
      } catch (InterruptedException e) {
        LOG.info("Interrupted when waiting container to exit");
        break;
      }

      if(waitThread.isAlive()){
        // container is still running, keep waiting
        continue;
      }
      else{
        LOG.info(String.format("Container %s running for %d secs and stopped.",
                containerId, waitedMilliSecs/1000));
        break;
      }
    }

    if(param.debugFlag) {
      LOG.info(String.format("Container %s is NOT removed as requested by user with debugFlag=%b",
              containerId, param.debugFlag));
    }else{
      docker.removeContainerCmd(containerId).exec();
      LOG.info(String.format("Container %s removed.", containerId));
      containerId = null;
    }

    return exitcode;
  }

  private void doStopContainer(String reason) {
    if (this.containerStopped) {
      return;
    }

    if(this.containerId == null)
      throw new IllegalStateException("containerId is null when call doStopContainer");


    LOG.info(String.format("Stopping Container %s cause %s", containerId, reason));

    // When stopping container, we just send the request to docker service,
    // and continue wait the waitThread to exit, which in turn wait the docker container exit.
    // If something wrong, cause the container never exit, so our process is blocked forever,
    // we just let it be. This situation need to be handled by the admins.
    StopContainerCmd stopContainerCmd = docker.stopContainerCmd(containerId);
    stopContainerCmd.withTimeout(stopTimeout);

    LOG.info(String.format("Executing stop command: %s", stopContainerCmd.toString()));
    try {
      stopContainerCmd.exec();
    }catch(com.github.dockerjava.api.NotFoundException nfe){
      handleDockerException(nfe);
    }catch(NotModifiedException nme){
      handleDockerException(nme);
    }
  }

  private void handleDockerException(DockerException e) {
    LOG.warn(e);
  }

  private int runTask(String containerName) throws IOException, DockerException {
    startContainer(containerName);
    return waitContainerExit();
  }

  private DockerClient createDockerClient() {
    LOG.info("Initializing Docker Client");
    DockerClientConfig.DockerClientConfigBuilder configBuilder = DockerClientConfig
            .createDefaultConfigBuilder();
    configBuilder.withLoggingFilter(this.param.debugFlag)
           .withUri("" + param.dockerHost);
            //.withDockerCertPath(param.dockerCertPath);
    DockerClientConfig config = configBuilder.build();

    return DockerClientBuilder.getInstance(config)
            .build();
  }

  private CreateContainerCmd getCreateContainerCmd(String containerName) {

    CreateContainerCmd con = docker.createContainerCmd(this.param.dockerImage);

      Options opts = new Options();
      opts.addOption(OptionBuilder.withLongOpt("rm").withDescription("rm the container after execute").create());
      opts.addOption(new Option("v","volume",true,"the memory of container"));
      opts.addOption(new Option("m","memory",true,"the memory of container"));
      opts.addOption(new Option("c","cpu-shares",true,"the cpu of the container"));
	  opts.addOption(new Option("H","net",true,"the host of the container"));
      CommandLine dockerArgsParser = null;
      try {
          dockerArgsParser = new GnuParser().parse(opts, param.getDockerArgs(), true);
      } catch (ParseException e) {
          e.printStackTrace();
      }

      if (dockerArgsParser.hasOption("m")) {

          String memoryArgs = dockerArgsParser.getOptionValue("m") ;
          LOG.info("Set container memory to " +memoryArgs);
          int memorySize = Integer.parseInt(memoryArgs.split("[\\D]+")[0]);
          if(memoryArgs.contains("m")||memoryArgs.contains("M")){
              con.withMemoryLimit(new Long( memorySize * 1024 * 1024L ));

          }else if(memoryArgs.contains("g")||memoryArgs.contains("G")) {
              con.withMemoryLimit(new Long( memorySize * 1024 * 1024* 1024L));

          }
      }else {

          con.withMemoryLimit(DEFAULT_CONTAINER_MEMORY);

      }

      if(dockerArgsParser.hasOption("c")){
          int cpushares = Integer.parseInt(dockerArgsParser.getOptionValue("c"));
          con.withCpuShares(cpushares);
      }else{
          con.withCpuShares(DEFAULT_CONTAINER_CPU_SHARES);
      }

	  if(dockerArgsParser.hasOption("H")){
        String net = dockerArgsParser.getOptionValue("H");
        con.withNetworkMode(net);
      }
	

      con.withName(containerName);
   // con.withCpuShares(this.param.containerVirtualCores);
   // con.withMemoryLimit(new Long(this.param.containerMemory * 1024 * 1024 * 1024));
    con.withAttachStderr(true);
    con.withAttachStdin(false);
    con.withAttachStdout(true);

    this.volumeBinds.add(new Bind(param.runnerScriptPath,
            new Volume(CONTAINER_RUNNER_SCRIPT_PATH), AccessMode.ro));
    for(String mountPath :param.mountVolume.split("\\+"))
    {
        Bind localPath = new Bind(mountPath.split(":")[0],new Volume(mountPath.split(":")[1]),AccessMode.rw);
        volumeBinds.add(localPath);


    }
    con.withBinds(volumeBinds.toArray(new Bind[volumeBinds.size()]));

    ArrayList<String> cmds = new ArrayList<String>();
    Collections.addAll(cmds, RUN_CMD);
    Collections.addAll(cmds, param.cmdAndArgs);
    param.cmdAndArgs = cmds.toArray(param.cmdAndArgs);
    con.withCmd(this.param.cmdAndArgs);
    con.withWorkingDir(param.workingDir);



    return con;
  }

  public void shutdown() throws IOException {
    LOG.info("Finishing");

    // Container should be stopped first
    if(!containerStopped) {
      LOG.warn(String.format("Docker Container not stopped when shutting down, will stop it now",
              containerId));
      stopContainer();
      waitContainerExit();
    }

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
    Thread thread = new Thread() {
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
          while ((line = reader.readLine()) != null) {
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

    thread.setDaemon(true);
    return thread;
  }

  public static void main(String[] args) {

    int result = -1;
    try {
      DockerContainerRunnerParam dockerContainerRunnerParam = new DockerContainerRunnerParam();
      try {
        dockerContainerRunnerParam.initFromCmdlineArgs(args);
        if (dockerContainerRunnerParam.isPrintHelp) {
          dockerContainerRunnerParam.printUsage();
          System.exit(ExitCode.SUCC.getValue());
        }
      } catch (IllegalArgumentException e) {
        System.err.println(e.getLocalizedMessage());
        dockerContainerRunnerParam.printUsage();
        System.exit(ExitCode.ILLEGAL_ARGUMENT.getValue());
      }

      DockerContainerRunner client = new DockerContainerRunner(dockerContainerRunnerParam);

      result = client.runTask(String.format("dockerClientRunner-%d", System.currentTimeMillis()));

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

  public void stopContainer(){
    isStopContainerRequested = true;
  }

  public float getProgress() {
    // TODO Implement getProgress
    return 0;
  }

}
