package com.sogou.docker.client.model;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by guoshiwei on 15/5/10.
 */
public class DistributiedDockerConfiguration extends Configuration {
  private static String DISTRIBUTED_DOCKER_CONFIGURATION_FILE
          = "distributed-docker-site.xml";

  private static String DDOCKER_PREFIX = "ddocker.";
  public static String DDOCKER_RUNNER_PATH = DDOCKER_PREFIX + "runner.path";

  static {
    Configuration.addDefaultResource(DISTRIBUTED_DOCKER_CONFIGURATION_FILE);
  }
}
