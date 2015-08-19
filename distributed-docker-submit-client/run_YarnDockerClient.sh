 export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_71.jdk/Contents/Home
 
 CP=.
 while read line
 do
     if [ ${line:0:1} == "#" ]; then
	 continue
     fi
    CP=$CP:$line
 done <readable.classpath.txt
 java -cp $CP com.sogou.docker.client.docker.YarnDockerClient -container_memory 512 -container_vcores 1 -docker_image registry.docker.dev.sogou-inc.com:5000/clouddev/sogou-rhel-base -workingdir / -runner_path /Users/guoshiwei/DEV/docker-client/runner.py echo hello
