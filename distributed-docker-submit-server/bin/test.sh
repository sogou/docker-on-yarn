


curl -H "Accept: application/json" -H "Content-type: application/json" -X POST \
-d '{"virualdir":"/search/working/", "docker_image":"10.134.69.187:5000/hadoop/cdh5client:1.2", "workingdir":"/search/sogouquery/", "command":"sh time_website.sh abcde 20141001"}'  \
 http://127.0.0.1:18088/docker/submit
