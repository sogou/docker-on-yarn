package com.sogou.dockeronyarn.client;

import com.google.common.base.Preconditions;
import com.sogou.dockeronyarn.appmaster.DockerRunnerApplicationMaster;
import com.sogou.dockeronyarn.common.DistributedDockerConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.mortbay.util.StringUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by guoshiwei on 15/4/25.
 */
public class DockerOnYarnClient {
  private static final Log LOG = LogFactory.getLog(DockerOnYarnClient.class);

  // Configuration
  private final Configuration yarnConf;

  private final DistributedDockerConfiguration ddockerConf;
  private final YarnClient yarnClient;

  // Hardcoded path to custom log_properties
  private static final String log4jHdfsPath = "log4j.properties";

  // Main class to invoke application master
  private final String appMasterMainClass;

  private boolean keepContainers;

  private static final String appMasterJarHdfsPath = "AppMaster.jar";

  // Amt. of memory resource to request for to run the App Master
  private int amMemory = 10;
  // Amt. of virtual core resource to request for to run the App Master
  private int amVCores = 1;


  private boolean debugFlag;
  private FileSystem fs;

  public DockerOnYarnClient() {
    this.yarnConf = new YarnConfiguration();
    this.ddockerConf = new DistributedDockerConfiguration();
    this.appMasterMainClass = DockerRunnerApplicationMaster.class.getName();
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConf);

  }

  private String getAppMasterJarfilePath() {
    return null;
  }

  public void start() throws IOException, YarnException {
    LOG.info("Running Client");
    fs = FileSystem.get(yarnConf);
    yarnClient.start();
    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    LOG.info("Got Cluster metric info from ASM"
            + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
            NodeState.RUNNING);
    LOG.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodeReports) {
      LOG.info("Got node report from ASM for"
              + ", nodeId=" + node.getNodeId()
              + ", nodeAddress" + node.getHttpAddress()
              + ", nodeRackName" + node.getRackName()
              + ", nodeNumContainers" + node.getNumContainers());
    }

    List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        LOG.info("User ACL Info for Queue"
                + ", queueName=" + aclInfo.getQueueName()
                + ", userAcl=" + userAcl.name());
      }
    }
  }


  public void stop() {
    yarnClient.stop();
  }

  /**
   * Submit app descripted by @appDescriptor and wait it for shutdown.
   */
  public boolean run(DockerOnYarnAppDescriptor appDescriptor) throws IOException, YarnException {
  //  LOG.info("Running Client");
   // yarnClient.start();
    start();
    ApplicationId appId = submitYarnApplication(appDescriptor);
    return monitorApplication(appId, appDescriptor);
  }

  public ApplicationId submitYarnApplication(DockerOnYarnAppDescriptor appDescriptor) throws IOException, YarnException {

    QueueInfo queueInfo = yarnClient.getQueueInfo(appDescriptor.getAmQueue());
    LOG.info("Queue info"
            + ", queueName=" + queueInfo.getQueueName()
            + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
            + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
            + ", queueApplicationCount=" + queueInfo.getApplications().size()
            + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

    // Get a new application id
    YarnClientApplication app = yarnClient.createApplication();

    int maxMem = app.getNewApplicationResponse().getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    ApplicationSubmissionContext appContext = createApplicationSubmissionContext(appDescriptor, app);

    LOG.info("Submitting application to ASM");
    return yarnClient.submitApplication(appContext);
  }

  private ApplicationSubmissionContext createApplicationSubmissionContext(
          DockerOnYarnAppDescriptor appDescriptor, YarnClientApplication app) throws IOException {
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

    appContext.setApplicationName(appDescriptor.getAppName());
    appContext.setPriority(createPriorityRecord(appDescriptor));
    appContext.setQueue(appDescriptor.getAmQueue());

    appContext.setAMContainerSpec(createContainerLaunchContext(appDescriptor, appContext));

    return appContext;
  }

  private Priority createPriorityRecord(DockerOnYarnAppDescriptor appDescriptor) {
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(appDescriptor.getAmPriority());
    return pri;
  }

  /**
   * Setup required resources for AM.
   *
   * All required resources should be placed on HDFS first.
   * The value of DistributedDockerConfiguration.REQUIRED_RESOURCE_HDFS_PATH will be used.
   * You can set a value other than the default one to use different version of AppMaster.
   *
   * Under REQUIRED_RESOURCE_HDFS_PATH dir, must have the following file structure:
   *   - lib/appmaster.jar
   *   - runner.py
   *   - conf/distributed-docker-default.xml
   *   - conf/log4j.properties
   *
   * If a local log4j.properties is given, the default one on hdfs will not be used,
   * but the local one will be uploaded every time the app is submitted. Upload every time
   * is slow, so we prefer to use the default one.
   *
   * @param appDescriptor
   * @param appId
   * @return
   * @throws IOException
   */
  private Map<String, LocalResource> createLocaleResourceMap(
          DockerOnYarnAppDescriptor appDescriptor, ApplicationId appId) throws IOException {
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    LOG.info("Copy App Master jar from local filesystem and add to local environment");
    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path

    addToLocalResources(appDescriptor.getAmJarPath(), appMasterJarHdfsPath, appId.toString(),
            localResources, appDescriptor);

    addToLocalResources(ddockerConf.get(DistributedDockerConfiguration.DDOCKER_RUNNER_PATH),
            DockerRunnerApplicationMaster.LOCAL_RUNNER_NAME, appId.toString(),
            localResources, appDescriptor);

    // Set the log4j properties if needed
    if (!appDescriptor.getLog4jPropFile().isEmpty()) {
      addToLocalResources(appDescriptor.getLog4jPropFile(), log4jHdfsPath, appId.toString(),
              localResources, appDescriptor);
    }
    return localResources;
  }

  private ContainerLaunchContext createContainerLaunchContext(
          DockerOnYarnAppDescriptor appDescriptor,
          ApplicationSubmissionContext appContext) throws IOException {
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);


    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = createLocaleResourceMap(appDescriptor, appContext.getApplicationId());

    amContainer.setLocalResources(localResources);

    // Set the env variables to be setup in the env where the application master will be run
    LOG.info("Set the environment for the application master");
    Map<String, String> env = getEnv();

 //  env.put("mount.volume",appDescriptor.getMountVolume());

    amContainer.setEnvironment(env);
    amContainer.setCommands(getAppMasterCommands(appDescriptor));

    // Set up resource type requirements
    // For now, both memory and vcores are supported, so we set memory and
    // vcores requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMemory);
    capability.setVirtualCores(amVCores);
    appContext.setResource(capability);

    // Setup security tokens
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = new Credentials();
      String tokenRenewer = yarnConf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException(
                "Can't get Master Kerberos principal for the RM to use as renewer");
      }

      // For now, only getting tokens for the default file-system.
      final org.apache.hadoop.security.token.Token<?> tokens[] =
              fs.addDelegationTokens(tokenRenewer, credentials);
      if (tokens != null) {
        for (org.apache.hadoop.security.token.Token<?> token : tokens) {
          LOG.info("Got dt for " + fs.getUri() + "; " + token);
        }
      }
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContainer.setTokens(fsTokens);
    }
    return amContainer;
  }

  private List<String> getAppMasterCommands(DockerOnYarnAppDescriptor appDescriptor) {
    // Set the necessary command to execute the application master
    List<String> vargs = new ArrayList<String>(30);

    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add("java");

    // Set Xmx based on am memory size
    vargs.add("-Xmx" + amMemory + "m");
 //  vargs.add("-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=12345");

    // Pass DistributedDockerConfiguration as Properties
    for (Map.Entry<String, String> e : ddockerConf) {
      if (e.getKey().startsWith(DistributedDockerConfiguration.DDOCKER_PREFIX)) {
        vargs.add(String.format("-D%s=%s", e.getKey(), e.getValue()));
      }
    }

    // Set class name
    vargs.add(appMasterMainClass);

    // Set params for Application Master
    vargs.add("-job_name");
    vargs.add(appDescriptor.getAppName());


    vargs.add("-v");
    vargs.add(appDescriptor.getMountVolume());

    vargs.add("-w");
      vargs.add(appDescriptor.getWorkDir());

    vargs.add("--docker-args");
    vargs.add("'"+StringUtils.join(appDescriptor.getDockerArgs()," ")+"\'");

    vargs.add("-image");
    vargs.add(appDescriptor.getDockerImage());
    if (debugFlag) {
      vargs.add("--debug");
    }

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

    vargs.add(appDescriptor.getCommandToRun());

    Object object ;
    LOG.info("AppMasterCommand: " + StringUtils.join(vargs, " "));

    return vargs;
  }

  private Map<String, String> getEnv() {
    Map<String, String> env = new HashMap<String, String>();

    StringBuilder classPathEnv = new StringBuilder("./*");
    for (String c : yarnConf.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(File.pathSeparatorChar).append(c.trim());
    }

    classPathEnv.append(File.pathSeparatorChar).append(
            ApplicationConstants.Environment.CLASSPATH.$());


    // add the runtime classpath needed for tests to work
    if (yarnConf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathEnv.append(':');
      classPathEnv.append(System.getProperty("java.class.path"));
    }

    env.put("CLASSPATH", classPathEnv.toString());
    return env;
  }

  public YarnClient getYarnClient() {
    return yarnClient;
  }

  /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   *
   * @param appId Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnException
   * @throws IOException
   */
  public boolean monitorApplication(ApplicationId appId, DockerOnYarnAppDescriptor appDescriptor)
          throws YarnException, IOException {

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);

      LOG.info("Got application report from ASM for"
              + ", appId=" + appId.getId()
              + ", clientToAMToken=" + report.getClientToAMToken()
              + ", appDiagnostics=" + report.getDiagnostics()
              + ", appMasterHost=" + report.getHost()
              + ", appQueue=" + report.getQueue()
              + ", appMasterRpcPort=" + report.getRpcPort()
              + ", appStartTime=" + report.getStartTime()
              + ", yarnAppState=" + report.getYarnApplicationState().toString()
              + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
              + ", appTrackingUrl=" + report.getTrackingUrl()
              + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          LOG.info("Application did finished unsuccessfully."
                  + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                  + ". Breaking monitoring loop");
          return false;
        }
      } else if (YarnApplicationState.KILLED == state
              || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not shutdown."
                + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                + ". Breaking monitoring loop");
        return false;
      }

      if (System.currentTimeMillis() > (appDescriptor.getClientStartTime() + appDescriptor.getClientTimeout())) {
        LOG.info("Reached client specified timeout for application. Killing application");
        forceKillApplication(appId);
        return false;
      }
    }

  }

  /**
   * Kill a submitted application by sending a call to the ASM
   *
   * @param appId Application Id to be killed.
   * @throws YarnException
   * @throws IOException
   */
  public void forceKillApplication(ApplicationId appId)
          throws YarnException, IOException {
    // TODO clarify whether multiple jobs with the same app id can be submitted and be running at
    // the same time.
    // If yes, can we kill a particular attempt only?

    // Response can be ignored as it is non-null on success or
    // throws an exception in case of failures
    yarnClient.killApplication(appId);
  }

  private void addToLocalResources(String fileSrcPath,
                                   String fileDstPath, String appId,
                                   Map<String, LocalResource> localResources,
                                   DockerOnYarnAppDescriptor appDescriptor) throws IOException {
    Preconditions.checkNotNull(fileDstPath);

    // TODO Skip upload appMaster resources if it is not changed.
    String suffix =
            appDescriptor.getAppName() + "/" + appId + "/" + fileDstPath;
    Path dst =
            new Path(fs.getHomeDirectory(), suffix);

    fs.copyFromLocalFile(new Path(fileSrcPath), dst);

    FileStatus scFileStatus = fs.getFileStatus(dst);
    LocalResource scRsrc =
            LocalResource.newInstance(
                    ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                    LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
                    scFileStatus.getLen(), scFileStatus.getModificationTime());
    localResources.put(fileDstPath, scRsrc);
  }

  public void setDebugFlag(boolean debugFlag) {
    this.debugFlag = debugFlag;
  }



  public ApplicationReport getApplicationReport(ApplicationId appId)
  {
      try {
          return yarnClient.getApplicationReport(appId);
      } catch (YarnException e) {
          e.printStackTrace();

      } catch (IOException e) {
          e.printStackTrace();
      }
      return null ;
  }

  public void killApplication(ApplicationId appId)
            throws YarnException, IOException {
        // TODO clarify whether multiple jobs with the same app id can be submitted and be running at
        // the same time.
        // If yes, can we kill a particular attempt only?

        // Response can be ignored as it is non-null on success or
        // throws an exception in case of failures
        yarnClient.killApplication(appId);
  }



}
