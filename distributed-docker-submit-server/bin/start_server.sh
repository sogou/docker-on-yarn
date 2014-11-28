#!/bin/sh
project=docker-submit-server
main_class=com.sogou.docker.server.DockerSubmitMain

crontab=$(cat "/var/spool/cron/root" | grep "monitor_docker_submit_server" | wc -l)
if [ $crontab -eq 0 ]
 then
    echo "*/1 * * * * cd /search/eclipse/distributed-docker-submit-server/; sh monitor_docker_submit_server.sh >> monitor.log 2>&1" >> /var/spool/cron/root
fi


running_num=`jps -ml | grep -c $main_class`
if [ $running_num -ne 0 ]
then
	echo "$project $main_class is already running..."
	exit 0
fi

dir='/search/eclipse/workspace/distributed-docker-submit-server/target'
dir=`cd $dir/; pwd`

bin_dir=$dir/bin
lib_dir=$dir/lib
conf_dir=$dir/conf
log_dir=$dir/log
cp *.properties target/
cp target/*.jar target/lib/
rm -fr $lib_dir/hadoop*

JOBQUEUE_WORKER_HEAP_SIZE=128m

for f in $lib_dir/*
do
	CLASSPATH=$f:$CLASSPATH
done

CLASSPATH=$conf_dir:$CLASSPATH:`hadoop classpath`

# set JVM heap size
HEAP_SIZE=512m
if [ "x${JOBQUEUE_WORKER_HEAP_SIZE}" != "x" ]
then
	HEAP_SIZE=$JOBQUEUE_WORKER_HEAP_SIZE
fi
JAVA_OPTS="-Xmx${HEAP_SIZE}"

# set jmx remote port
JMX_REMOTE_OPTS=""
JAVA_OPTS="$JAVA_OPTS $JMX_REMOTE_OPTS"

cd $dir
echo classpath: $CLASSPATH
#nohup java $JAVA_OPTS -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=17891 -cp $CLASSPATH $main_class >$log_dir/$project.log 2>&1 &
nohup java $JAVA_OPTS   -cp $CLASSPATH $main_class >>submit.log &
code=$?
echo 'code: ' $code
exit $code

