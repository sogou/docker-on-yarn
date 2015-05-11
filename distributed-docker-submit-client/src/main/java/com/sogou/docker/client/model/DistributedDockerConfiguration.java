package com.sogou.docker.client.model;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * Created by guoshiwei on 15/5/10.
 */
public class DistributedDockerConfiguration extends Configuration {
  private static String DISTRIBUTED_DOCKER_CONFIGURATION_FILE
          = "distributed-docker-site.xml";

  public static String DDOCKER_PREFIX = "ddocker.";
  public static String DDOCKER_RUNNER_PATH = DDOCKER_PREFIX + "runner.path";

  public static final String CONTAINER_MEMORY = DDOCKER_PREFIX + "container.memeory";
  public static final int DEFAULT_CONTAINER_MEMORY = 512; // MB

  public static final String CONTAINER_CORES = DDOCKER_PREFIX + "container.cores";
  public static final int DEFAULT_CONTAINER_CORES = 1;

  public static final String DOCKER_CERT_PATH = DDOCKER_PREFIX + "docker.cert.path";

  public static final String DOCKER_HOST = DDOCKER_PREFIX + "docker.host";
  public static final String DEFAULT_DOCKER_HOST = "127.0.0.1:2376";

  static {
    Configuration.addDefaultResource(DISTRIBUTED_DOCKER_CONFIGURATION_FILE);
  }

  /**
   * Load system properties which startswith `DDOCKER_PREFIX`
   */
  public void loadSystemProperties(){
    final Properties properties = System.getProperties();
    for(String p: properties.stringPropertyNames()){
      if(p.startsWith(DDOCKER_PREFIX)){
        set(p, properties.getProperty(p));
      }
    }
  }

  public DistributedDockerConfiguration() {
    super();
    loadSystemProperties();
  }
}
