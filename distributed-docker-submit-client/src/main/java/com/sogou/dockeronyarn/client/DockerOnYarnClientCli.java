package com.sogou.dockeronyarn.client;

import com.sogou.dockeronyarn.common.Log4jPropertyHelper;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.sogou.dockeronyarn.common.Utils.checkNotEmpty;

/**
 * Created by guoshiwei on 15/5/16.
 */
public class DockerOnYarnClientCli {
  private static final Log LOG = LogFactory.getLog(DockerOnYarnClientCli.class);
  private final DockerClientV2 dockerClient;
  private DockerOnYarnAppDescriptor appDescriptor;
  private Options opts;

  public DockerOnYarnClientCli() {
    dockerClient = new DockerClientV2();
    this.setupOpts();
  }

  public static void main(String [] args){
    boolean result = false;
    try {
      DockerOnYarnClientCli client = new DockerOnYarnClientCli();
      LOG.info("Initializing Client");
      try {
        boolean doRun = client.init(args);
        if (!doRun) {
          System.exit(0);
        }
      } catch (IllegalArgumentException e) {
        System.err.println(e.getLocalizedMessage());
        client.printUsage();
        System.exit(-1);
      }
      result = client.dockerClient.run(client.appDescriptor);
    } catch (Throwable t) {
      LOG.fatal("Error running CLient", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application completed successfully");
      System.exit(0);
    }
    LOG.error("Application failed to complete successfully");
    System.exit(2);
  }

  private void setupOpts(){
    opts = new Options();
    opts.addOption("appname", true, "Application Name. Default value - DistributedShell");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
    opts.addOption("timeout", true, "Application timeout in milliseconds");
    opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
    opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
    opts.addOption("jar", true, "Jar file containing the application master");

    opts.addOption("log_properties", true, "log4j.properties file");
    opts.addOption("container_retry", true, "container retry count");
    opts.addOption("image", true, "docker image name");

    opts.addOption("debug", false, "Dump out debug information");
    opts.addOption("help", false, "Print usage");
  }

  /**
   * Parse command line options
   * @param args Parsed command line options
   * @return Whether the init was successful to run the client
   * @throws ParseException
   */
  public boolean init(String[] args) throws ParseException {

    CommandLine cliParser = new GnuParser().parse(opts, args, true);

    if (args.length == 0) {
      throw new IllegalArgumentException("No args specified for client to initialize");
    }

    if (cliParser.hasOption("log_properties")) {
      String log4jPath = cliParser.getOptionValue("log_properties");
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(DockerClient.class, log4jPath);
      } catch (Exception e) {
        LOG.warn("Can not set up custom log4j properties. " + e);
      }
    }

    if (cliParser.hasOption("help")) {
      printUsage();
      return false;
    }

    if (cliParser.hasOption("debug")) {
      dockerClient.setDebugFlag(true);
    }

    appDescriptor.setAppName(cliParser.getOptionValue("appname", "DistributedShell"));
    appDescriptor.setAmPriority(Integer.parseInt(cliParser.getOptionValue("priority", "0")));
    appDescriptor.setAmQueue(cliParser.getOptionValue("queue", "default"));

    appDescriptor.setContainer_retry(Integer.parseInt(cliParser.getOptionValue("container_retry", "3")));

    appDescriptor.setClientTimeout(Integer.parseInt(cliParser.getOptionValue("timeout", "600000")));

    appDescriptor.setLog4jPropFile(cliParser.getOptionValue("log_properties", ""));

    String dockerImage = cliParser.getOptionValue("image");
    checkNotEmpty(dockerImage, "No dockerImage given");
    appDescriptor.setDockerImage(dockerImage);

    String commandToRun = StringUtils.join(cliParser.getArgs(), " ");
    checkNotEmpty(commandToRun, "No command given");

    appDescriptor.setCommandToRun(commandToRun);
    return true;
  }

  private void printUsage() {
    new HelpFormatter().printHelp("[options] command [commandArgs]", opts);
  }
}
