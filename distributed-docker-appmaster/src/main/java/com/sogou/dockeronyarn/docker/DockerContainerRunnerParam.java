package com.sogou.dockeronyarn.docker;

import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DockerContainerRunnerParam {// Debug flag
  public boolean debugFlag = false;// Args to be passed to the shell command
  public String[] cmdAndArgs;// Env variables to be setup for the shell command
  public Map<String, String> cmdEnv = new HashMap<String, String>();
  public String dockerImage;
  public String dockerHost;
  public String dockerCertPath;
  public  String mountVolume;


  public String workingDir;

  //the path to be mounted into docker container,eg: {"/root/ugi_config:/root/uig_config"}
  private String[] mountPaths ;
  public String[] virtualDirs;// memory to request for container in docker will be executed
  public long containerMemory = 10;// virtual cores to request for container in docker will be executed,to do

    public String[] getDockerArgs() {
        return dockerArgs;
    }

    public void setDockerArgs(String[] dockerArgs) {
        this.dockerArgs = dockerArgs;
    }

    public int containerVirtualCores = 512;// Timeout threshold for client. Kill app after time interval expires.
  public long clientTimeout = 24 * 3600 * 1000;
  public String runnerScriptPath; // The absolute path of runner script on host local filesystem
  private Options opts = new Options();
  public boolean isPrintHelp = false; // TODO: get value from command line options.
  private String[] dockerArgs = {};


  public DockerContainerRunnerParam(){
    opts.addOption("timeout", true, "Application timeout in milliseconds");
    opts.addOption("docker_image", true, "docker image to be executed");
    opts.addOption("docker_host", true, "ip:port of docker server, example: localhost:2376");
    opts.addOption("docker_cert_path", true, "Example: /some/path/certs");

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

  private static void requireOptionValue(String optionValue, String optionName){
    if(optionValue == null || optionValue.trim().isEmpty()){
      throw new IllegalArgumentException("No value given for '" + optionName
      + "' option.");
    }
  }
  public void initFromCmdlineArgs(String[] args) throws ParseException {

    Options opts = getOptions();
    CommandLine cmdLine = new GnuParser().parse(opts, args);
    ArrayList<String> cmds = new ArrayList<String>();

    if (args.length == 0) {
      throw new IllegalArgumentException(
              "No args specified for DockerContainerRunner to initialize");
    }

    if (!cmdLine.hasOption("docker_image")) {
      throw new IllegalArgumentException(
              "No image specified for DockerContainerRunner to initialize");
    } else {
      dockerImage = cmdLine.getOptionValue("docker_image");
      if (dockerImage == null
              || dockerImage.trim().length() == 0) {
        throw new IllegalArgumentException(
                "No image specified for DockerContainerRunner to initialize");
      }
    }
    dockerHost = cmdLine.getOptionValue("docker_host");
    requireOptionValue(dockerHost, "docker_host");

    dockerCertPath = cmdLine.getOptionValue("docker_cert_path");
    requireOptionValue(dockerCertPath, "docker_cert_path");

    runnerScriptPath = cmdLine.getOptionValue("runner_path", null);

    if (runnerScriptPath == null || runnerScriptPath.trim().length() == 0) {
      throw new IllegalArgumentException(
              "No runpath specified for DockerContainerRunner to run cmds");
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
              " Illegal timeout specified for DockerContainerRunner to initialize");
    }

    cmdAndArgs = cmdLine.getArgs();
  }

  private Options getOptions() {

    return opts;
  }

  public  void printUsage() {
    new HelpFormatter().printHelp("DockerContainerRunner [options] command [args ... ]", opts);
  }
}