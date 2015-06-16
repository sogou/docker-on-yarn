# 关于Docker容器相关垃圾的清理

* 已退出的Container，虽然DockerRunnerApplicationMaster会在container退出时删除，但还是可能由于某种原因没有清理，变为垃圾。
可启动一定时流程，清理一定时间（如一周）前退出的container
* 如果是Container中应用，通过volume挂载向外输出的中间文件，则应确保volume挂载的是YARN AppCache目录下的子目录，因而交给YARN来清理

# TODO

* [DONE] Integrate History Server
* [DONE] Implement docker client
* [CANCEL] Implement AM Tracking url
* [DONE] runner.py 通过YarnClient 指定
* [DONE] 给这个新Framework想个名字 DistributedDocker
* [DONE] 常用配置通过Config文件指定
* [DONE] Client提供submit接口，并提供异步追踪进度的方法

