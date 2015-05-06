package com.sogou.docker.client.docker;

import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class YarnDockerClientParam {// Debug flag
  boolean debugFlag = false;// Args to be passed to the shell command
  String[] cmdAndArgs;// Env variables to be setup for the shell command
  Map<String, String> cmdEnv = new HashMap<String, String>();
  public String dockerImage;
  public String workingDir;
  public String[] virtualDirs;// memory to request for container in docker will be executed
  long containerMemory = 10;// virtual cores to request for container in docker will be executed,to do
  int containerVirtualCores = 1;// Timeout threshold for client. Kill app after time interval expires.
  long clientTimeout = 24 * 3600 * 1000;
  public String runnerScriptPath; // The absolute path of runner script on host local filesystem
  private Options opts = new Options();

  public YarnDockerClientParam(){
    opts.addOption("timeout", true, "Application timeout in milliseconds");
    opts.addOption("docker_image", true, "docker image to be executed");

    opts.addOption("runner_path", true, "The absolute path of runner script on host local filesystem");
    opts.addOption("workingdir", true, "working dir for command");
    opts.addOption("cmd_env", true,
            "Environment. Specified as env_key=env_val pairs");
    opts.addOption("container_memory", true,
            "Amount of memory in MB to be requested to run the docker container");
    opts.addOption("container_vcores", true,
            "Amount of virtual cores to be requested to run the docker container");
    opts.addOption("debug", false, "Dump out debug information");
  }
  public void initFromCmdlineArgs(String[] args) throws ParseException {

    Options opts = getOptions();
    CommandLine cmdLine = new GnuParser().parse(opts, args);
    ArrayList<String> cmds = new ArrayList<String>();

    if (args.length == 0) {
      throw new IllegalArgumentException(
              "No args specified for YarnDockerClient to initialize");
    }

    if (!cmdLine.hasOption("docker_image")) {
      throw new IllegalArgumentException(
              "No image specified for YarnDockerClient to initialize");
    } else {
      dockerImage = cmdLine.getOptionValue("docker_image");
      if (dockerImage == null
              || dockerImage.trim().length() == 0) {
        throw new IllegalArgumentException(
                "No image specified for YarnDockerClient to initialize");
      }
    }

    runnerScriptPath = cmdLine.getOptionValue("runner_path", null);

    if (runnerScriptPath == null || runnerScriptPath.trim().length() == 0) {
      throw new IllegalArgumentException(
              "No runpath specified for YarnDockerClient to run cmds");
    }


    workingDir = cmdLine.getOptionValue("workingdir",
            "/search");

    if (cmdLine.hasOption("debug")) {
      debugFlag = true;
      try {
        Thread.sleep(10000l);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    if (cmdLine.hasOption("cmd_env")) {
      String envs[] = cmdLine.getOptionValues("cmd_env");
      for (String env : envs) {
        env = env.trim();
        int index = env.indexOf('=');
        if (index == -1) {
          cmdEnv.put(env, "");
          continue;
        }
        String key = env.substring(0, index);
        String val = "";
        if (index < (env.length() - 1)) {
          val = env.substring(index + 1);
        }
        cmdEnv.put(key, val);
      }
    }

    containerMemory = Integer.parseInt(cmdLine.getOptionValue(
            "container_memory", "10"));
    containerVirtualCores = Integer.parseInt(cmdLine.getOptionValue(
            "container_vcores", "1"));

    if (containerMemory < 0 || containerVirtualCores < 0) {
      throw new IllegalArgumentException(
              "Invalid no. of containers or container memory/vcores specified,"
                      + " exiting." + " Specified containerMemory="
                      + containerMemory + ", containerVirtualCores="
                      + containerVirtualCores);
    }

    clientTimeout = Integer.parseInt(cmdLine.getOptionValue("timeout",
            "86400000"));

    if (clientTimeout <= 0) {
      throw new IllegalArgumentException(
              " Illegal timeout specified for YarnDockerClient to initialize");
    }

    cmdAndArgs = cmdLine.getArgs();
  }

  private Options getOptions() {

    return opts;
  }

  public  void printUsage() {
    new HelpFormatter().printHelp("YarnDockerClient [options] command [args ... ]", opts);
  }
}