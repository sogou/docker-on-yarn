#!/bin/bash
source ./hadoop-env.sh
export HADOOP_CLASSPATH="/search/distributed-docker-submit-project/distributed-docker-appmaster/target/distributed-docker-appmaster-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
AM_JAR=../distributed-docker-appmaster/target/distributed-docker-appmaster-0.0.1-SNAPSHOT-jar-with-dependencies.jar
CLIENT_JAR=target/distributed-docker-submit-client-0.0.1-SNAPSHOT.jar
sh -x /usr/lib/hadoop/bin/hadoop jar $CLIENT_JAR com.sogou.dockeronyarn.client.DockerOnYarnClientCli \
    -jar $AM_JAR \
    -appname  nimei  \
    -queue   root.hdfs  \
    -image registry.docker.dev.sogou-inc.com:5000/clouddev/sogou-rhel-base:6.5 \
    echo hello world

