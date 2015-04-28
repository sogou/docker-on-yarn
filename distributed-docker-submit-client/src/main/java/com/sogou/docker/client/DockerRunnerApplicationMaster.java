package com.sogou.docker.client;

import com.sogou.docker.client.docker.LocalDockerContainerRunner;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The DockerRunnerApplicationMaster DONOT require more container from RM.
 * It only create and run a new container to docker service on the local host
 * where the AM is launched.
 *
 * Created by guoshiwei on 15/4/25.
 */
public class DockerRunnerApplicationMaster {
  private static final Log LOG = LogFactory.getLog(DockerRunnerApplicationMaster.class);

  private Configuration conf;

  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRMClient;
  private ApplicationAttemptId appAttemptID;

  // TODO: Implement client rpc proctocal using REST API.
  // Hostname of the container
  private String appMasterHostname = "localhost";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = 9999;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = ""; //"http://localhost:9999/someurl";

  // Hardcoded path to custom log_properties
  private static final String log4jPath = "log4j.properties";
  private int requestPriority;
  protected AtomicInteger numRetryCount = new AtomicInteger();

  protected AtomicInteger MAX_RETRY_COUNT = new AtomicInteger(3);

  private ByteBuffer allTokens;

  private LocalDockerContainerRunner localDockerContainerRunner ;
  private volatile boolean done;

  public DockerRunnerApplicationMaster() {
    // Set up the configuration
    conf = new YarnConfiguration();
  }

  public static void main(String[] args) {
    boolean result = false;
    try {
      DockerRunnerApplicationMaster appMaster = new DockerRunnerApplicationMaster();
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      result = appMaster.run();
    } catch (Throwable t) {
      LOG.fatal("Error running ApplicationMaster", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  private boolean run() throws IOException, YarnException {
    LOG.info("Starting ApplicationMaster");

    Credentials credentials =
            UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(conf);
    amRMClient.start();


    // Setup local RPC Server to accept status requests directly from clients
    // TODO need to setup a protocol for client to be able to communicate to
    // the RPC server
    // TODO use the rpc port info to register with the RM for the client to
    // send requests to this app master

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    appMasterHostname = NetUtils.getHostname();
    RegisterApplicationMasterResponse response = amRMClient
            .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                    appMasterTrackingUrl);
    // Dump out information about cluster capability as seen by the
    // resource manager
    int maxMem = response.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);

    try{
      localDockerContainerRunner.run();
      Thread.sleep(60* 1000);

//      while (!done && !localDockerContainerRunner.isFinshed()) {
//        try {
//          Thread.sleep(200);
//        } catch (InterruptedException ex) {}
//      }
    }
    catch (Exception e){
      LOG.error("localDockerContainerRunner exited with exception: " + e.getMessage(), e);
    }
    finally {
      localDockerContainerRunner.ensureContainerRemoved();
    }
    // TODO Launch docker container, and wait it finished.
    return finish();
  }


  /**
   * Parse command line options
   *
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   */
  private boolean init(String[] args) throws ParseException {
    Options opts = new Options();
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("container_retry", true, "Application container_retry. Default 3");
    opts.addOption("debug", false, "Dump out debug information");

    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    //Check whether customer log4j.properties file exists
    if (fileExist(log4jPath)) {
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(DockerApplicationMaster_23.class,
                log4jPath);
      } catch (Exception e) {
        LOG.warn("Can not set up custom log4j properties. " + e);
      }
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    if (cliParser.hasOption("debug")) {
      dumpOutDebugInfo();
    }

    Map<String, String> envs = System.getenv();

    if (!envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
      if (cliParser.hasOption("app_attempt_id")) {
        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } else {
        throw new IllegalArgumentException(
                "Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ConverterUtils.toContainerId(envs
              .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
      appAttemptID = containerId.getApplicationAttemptId();
    }

    if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
      throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
              + " not set in the environment");
    }
    if (!envs.containsKey(ApplicationConstants.Environment.NM_HOST.name())) {
      throw new RuntimeException(ApplicationConstants.Environment.NM_HOST.name()
              + " not set in the environment");
    }
    if (!envs.containsKey(ApplicationConstants.Environment.NM_HTTP_PORT.name())) {
      throw new RuntimeException(ApplicationConstants.Environment.NM_HTTP_PORT
              + " not set in the environment");
    }
    if (!envs.containsKey(ApplicationConstants.Environment.NM_PORT.name())) {
      throw new RuntimeException(ApplicationConstants.Environment.NM_PORT.name()
              + " not set in the environment");
    }

    LOG.info("Application master for app" + ", appId="
            + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
            + appAttemptID.getApplicationId().getClusterTimestamp()
            + ", attemptId=" + appAttemptID.getAttemptId());


    requestPriority = Integer.parseInt(cliParser
            .getOptionValue("priority", "0"));

    this.MAX_RETRY_COUNT.set(Integer.parseInt(cliParser.getOptionValue(
            "container_retry", "3")));
    return initDockerContainerRunner();
  }

  private boolean initDockerContainerRunner() {
    this.localDockerContainerRunner = new LocalDockerContainerRunner();
    return true;
  }

  /**
   * Dump out contents of $CWD and the environment to stdout for debugging
   */
  private void dumpOutDebugInfo() {

    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
      System.out.println("System env: key=" + env.getKey() + ", val="
              + env.getValue());
    }

    BufferedReader buf = null;
    try {
      String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") :
              Shell.execCommand("ls", "-al");
      buf = new BufferedReader(new StringReader(lines));
      String line = "";
      while ((line = buf.readLine()) != null) {
        LOG.info("System CWD content: " + line);
        System.out.println("System CWD content: " + line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.cleanup(LOG, buf);
    }
  }

  private boolean finish(){
    // When the application completes, it should send a finish application
    // signal to the RM
    LOG.info("Application completed. Signalling finish to RM");

    FinalApplicationStatus appStatus;
    String appMessage = null;
    boolean success = true;
    if (localDockerContainerRunner.getExitStatus()  == 0) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics." + ", docker Container exited code: " +
      localDockerContainerRunner.getExitStatus();
      success = false;
    }

    try {
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException ex) {
      LOG.error("Failed to unregister application", ex);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }

    amRMClient.stop();
    return success;
  }

  private boolean fileExist(String filePath) {

    return new File(filePath).exists();
  }

  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler{

    @Override
    public void onContainersCompleted(List<ContainerStatus> list) {
      // Nothing to do since we do not require more container
    }

    @Override
    public void onContainersAllocated(List<Container> list) {
      // Nothing to do since we do not require more container
    }

    @Override
    public void onShutdownRequest() {
      done = true;
      localDockerContainerRunner.stop();
    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {
      // Nothing to do since we do not require more container
    }

    @Override
    public float getProgress() {

      return localDockerContainerRunner.getProgress();
    }

    @Override
    public void onError(Throwable throwable) {
      done = true;
      localDockerContainerRunner.stop();
      amRMClient.stop();
    }
  }
}
