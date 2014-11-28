flag=`jps -m | awk '{print "\$2"}' | grep -c '^DockerSubmitMain$'`
if [ $flag = 0 ]
then
	echo [`date`] Start docker submit server
	cd /search/eclipse/workspace/distributed-docker-submit-server/; sh start_server.sh
fi
