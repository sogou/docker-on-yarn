#!/bin/bash
source ./hadoop-env.sh

AM_JAR=target/distributed-docker-submit-client-0.0.1-SNAPSHOT-jar-with-dependencies.jar
CLIENT_JAR=$AM_JAR
~/DEV/ENV/hadoop-2.7.0/bin/hadoop jar $CLIENT_JAR com.sogou.docker.client.DockerClientV2 \
    -jar $AM_JAR $@
