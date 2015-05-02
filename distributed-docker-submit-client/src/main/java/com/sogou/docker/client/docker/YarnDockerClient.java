package com.sogou.docker.client.docker;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.NotModifiedException;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ListImagesCmd;
import com.github.dockerjava.api.command.LogContainerCmd;
import com.github.dockerjava.api.command.PullImageCmd;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.command.StopContainerCmd;
import com.github.dockerjava.api.command.WaitContainerCmd;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig.DockerClientConfigBuilder;
import com.github.dockerjava.jaxrs.DockerClientBuilder;


public class YarnDockerClient {

	private static final Log LOG = LogFactory.getLog(YarnDockerClient.class);

	private static  String[] RUN_CMD = new String[] {"/usr/bin/python", "/search/runner.py"};
	// Configuration
	private Configuration conf;
	private YarnClient yarnClient;
	// Command line options
	private Options opts;

	// Debug flag
	boolean debugFlag = false;

	// Args to be passed to the shell command
	private String[] cmdArgs;

	// Env variables to be setup for the shell command
	private Map<String, String> cmdEnv = new HashMap<String, String>();

	public String dockerImage;
	
	public String workingDir;
	public String[] virtualDirs;
	// memory to request for container in docker will be executed
	private long containerMemory = 10;
	// virtual cores to request for container in docker will be executed,to do
	private int containerVirtualCores = 1;

	// Timeout threshold for client. Kill app after time interval expires.
	private long clientTimeout = 24 * 3600 * 1000;
	
	private long streamTimeout = 10*1000;
	private int stopTimeout = 60;

	private Thread stdoutThread;

	private Thread stderrThread;

	private WaitTaskRunner wtr;

	private Thread waitThread;

	private String containerId;
	
	 private  volatile boolean containerStoped = false;

	private DockerClient docker;

	private Process pullProcess = null;
	
	private int pullProcessTryNum = 3;

	private String runPath;

	YarnDockerClient(Configuration conf) {
		this.conf = conf;
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		opts = new Options();
		
		opts.addOption("timeout", true, "Application timeout in milliseconds");
		opts.addOption("docker_image", true, "docker image to be executed");
		opts.addOption("virtualdir", true, "docker virtualdir  to bind host's dir");
		opts.addOption("runpath", true, "the shell to run cmd");
		opts.addOption("workingdir", true, "working dir for command");
		opts.addOption("cmd_args", true,
				"Command line args for the shell script."
						+ "Multiple args can be separated by empty space.");
		opts.addOption("cmd_env", true,
				"Environment. Specified as env_key=env_val pairs");
		opts.addOption("container_memory", true,
				"Amount of memory in MB to be requested to run the docker container");
		opts.addOption("container_vcores", true,
				"Amount of virtual cores to be requested to run the docker container");
		opts.addOption("debug", false, "Dump out debug information");
	}

	/**
		   */
	public YarnDockerClient() throws Exception {
		this(new YarnConfiguration());
	}

	public boolean init(String[] args) throws ParseException {

		CommandLine cliParser = new GnuParser().parse(opts, args);
		ArrayList<String> cmds = new ArrayList<String>();
		
		if (args.length == 0) {
			throw new IllegalArgumentException(
					"No args specified for YarnDockerClient to initialize");
		}

		if (!cliParser.hasOption("docker_image")) {
			throw new IllegalArgumentException(
					"No image specified for YarnDockerClient to initialize");
		} else {
			this.dockerImage = cliParser.getOptionValue("docker_image");
			if (this.dockerImage == null
					|| this.dockerImage.trim().length() == 0) {
				throw new IllegalArgumentException(
						"No image specified for YarnDockerClient to initialize");
			}
		}
		
		if (cliParser.hasOption("virtualdir")) 
		virtualDirs = cliParser.getOptionValues("virtualdir");
		
		if (cliParser.hasOption("runpath")) 
			runPath = cliParser.getOptionValue("runpath");
		
		if(runPath == null || runPath.trim().length() == 0){
			throw new IllegalArgumentException(
					"No runpath specified for YarnDockerClient to run cmds");
		}
		
		
		workingDir = cliParser.getOptionValue("workingdir",
				"/search");

		if (cliParser.hasOption("debug")) {
			debugFlag = true;
			try {
				Thread.sleep(10000l);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (cliParser.hasOption("cmd_args")) {
			cmdArgs = cliParser.getOptionValues("cmd_args");
		}else{
			throw new IllegalArgumentException(
					"No cmd specified for YarnDockerClient to exec");
		}
		
		for(int i = 0; i < RUN_CMD.length; ++i){
			cmds.add(RUN_CMD[i]);
		}
		cmds.add("cd");
		cmds.add(workingDir+ ";");
		
		for(int i = 0; i < cmdArgs.length; ++i){
			cmds.add(cmdArgs[i]);
		}
		cmdArgs = cmds.toArray(cmdArgs);
		
		if (cliParser.hasOption("cmd_env")) {
			String envs[] = cliParser.getOptionValues("cmd_env");
			for (String env : envs) {
				env = env.trim();
				int index = env.indexOf('=');
				if (index == -1) {
					cmdEnv.put(env, "");
					continue;
				}
				String key = env.substring(0, index);
				String val = "";
				if (index < (env.length() - 1)) {
					val = env.substring(index + 1);
				}
				cmdEnv.put(key, val);
			}
		}

		containerMemory = Integer.parseInt(cliParser.getOptionValue(
				"container_memory", "10"));
		containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(
				"container_vcores", "1"));

		if (containerMemory < 0 || containerVirtualCores < 0) {
			throw new IllegalArgumentException(
					"Invalid no. of containers or container memory/vcores specified,"
							+ " exiting." + " Specified containerMemory="
							+ containerMemory + ", containerVirtualCores="
							+ containerVirtualCores);
		}

		clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout",
				"86400000"));

		if (clientTimeout <= 0) {
			throw new IllegalArgumentException(
					" Illegal timeout specified for YarnDockerClient to initialize");
		}

		return true;
	}

	public int runtask() {
		DockerClientConfigBuilder configBuilder = DockerClientConfig
				.createDefaultConfigBuilder();
		configBuilder.withLoggingFilter(this.debugFlag);
		DockerClientConfig config = configBuilder.build();

		this.docker = DockerClientBuilder.getInstance(config)
				.build();
		
		boolean existed = assentPullImage();
		
		if(!existed){
			return  ExitCode.IMAGE_NOTFOUND.getValue();
		}
		
		
		CreateContainerCmd con = docker.createContainerCmd(this.dockerImage);
		con.withCpuShares(this.containerVirtualCores);
		con.withMemoryLimit(new Long(this.containerMemory * 1024 * 1024 ));
		con.withAttachStderr(true);
		con.withAttachStdin(true);
		con.withAttachStdout(true);
		con.withCmd(this.cmdArgs);
		final CreateContainerResponse response;
		try {
			response = con.exec();
		} catch (Exception e) {
			e.printStackTrace();
			return ExitCode.CONTAINER_NOT_CREATE.getValue();
		}
		this.containerId = response.getId();
		StartContainerCmd startCmd = docker.startContainerCmd(response.getId());
		
		bindHostDir(startCmd);
		
		try {
			startCmd.exec();
		} catch (NotFoundException e) {
			e.printStackTrace();
			return ExitCode.CONTAINER_NOT_CREATE.getValue();
		} catch (NotModifiedException e) {
			e.printStackTrace();
		}

		this.stdoutThread = new Thread() {
			@Override
			public void run() {
				BufferedReader reader = null;
				try {
					LogContainerCmd logCmd = docker.logContainerCmd(response
							.getId());
					logCmd.withFollowStream(true);
					logCmd.withStdErr(false);
					logCmd.withStdOut(true);
					logCmd.withTimestamps(true);
					
					InputStream input = logCmd.exec();
					reader = new BufferedReader(new InputStreamReader(input));
					String line;
					while ((line = reader.readLine()) != null && !isInterrupted()) {
						System.out.println(line);
					}

				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (reader != null) {
						try {
							reader.close();
							System.out.println("stdout closed");
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

				}
			}
		};

		 this.stderrThread = new Thread() {

			@Override
			public void run() {
				BufferedReader reader = null;
				try {
					LogContainerCmd logCmd = docker.logContainerCmd(response
							.getId());
					logCmd.withFollowStream(true);
					logCmd.withStdErr(true);
					logCmd.withStdOut(false);
					logCmd.withTimestamps(true);
					InputStream input = logCmd.exec();
					reader = new BufferedReader(new InputStreamReader(input));
					String line;
					while ((line = reader.readLine()) != null && !isInterrupted()) {
						
						System.err.println(line);
						
					}

				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (reader != null) {
						try {
							reader.close();
							System.out.println("stderr closed");
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

				}
			}
		};

		try {
			stdoutThread.start();
		} catch (IllegalThreadStateException e) {
		}

		try {
			stderrThread.start();
		} catch (IllegalThreadStateException e) {
		}

	     this.wtr = new WaitTaskRunner(docker, response.getId());
		 this.waitThread = new Thread(wtr, "waitThread");

		try {
			waitThread.start();
		} catch (IllegalThreadStateException e) {
		}
		int value;
		try {
			waitThread.join(this.clientTimeout);
		} catch (InterruptedException e) {
			System.out.println("container  interrupted");
			e.printStackTrace();
		}

		 value = wtr.getExitCode();
		 
		if(this.containerId != null && !this.containerStoped){
			containerStoped = true;
			StopContainerCmd stopContainerCmd = docker.stopContainerCmd(containerId);
			 stopContainerCmd.withTimeout(stopTimeout);
			try {
				stopContainerCmd.exec();
			} catch (Exception e) {
				
				LOG.info("docker container " + response.getId()
						+ " has been killed", e);
			}
			LOG.info("container  stoped by main");
			
		}
		
		try {
			stderrThread.join(this.streamTimeout);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			stdoutThread.join(this.streamTimeout);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			this.docker.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return value;
	}

	private void bindHostDir(StartContainerCmd startCmd) {
		// TODO Auto-generated method stub
		Bind[] binds = null;
		if(this.virtualDirs != null){
			binds = new Bind[this.virtualDirs.length + 1];
			File file = new File(Constants.DOCKER_USER_SPACE + "/" + this.containerId);
			boolean filesucc  = false;
			try{
				 filesucc = file.mkdir();
			}catch(Exception e){
				e.printStackTrace();
				throw new RuntimeException(
						"can not mkdir " + Constants.DOCKER_USER_SPACE + "/" + this.containerId + " exception: " + e.getMessage());
			}
			if(!filesucc){
				throw new RuntimeException(
						"fail to  mkdir " + Constants.DOCKER_USER_SPACE + "/" + this.containerId);
			}
			
			
			try{
				for(int i = 0 ; i < virtualDirs.length; ++i){
					binds[i] = Bind.parse(Constants.DOCKER_USER_SPACE + "/" + this.containerId+":" + this.virtualDirs[i]);
				}
				
				
			}catch(Exception e){
				e.printStackTrace();
				throw new IllegalArgumentException(
						" Illegal virtualdir specified for YarnDockerClient to run");
			}
		}
		
		if(binds == null){
			binds = new Bind[1];
		}
		System.out.println("runpath: " + runPath);
		binds[binds.length - 1] = Bind.parse(runPath+":rw");
		startCmd.withBinds(binds);
	}

	private boolean assentPullImage()  {
		int c = 0;
		int ret = -1;
		LOG.info("downloading image " + this.dockerImage);
		while(this.pullProcess == null && c < this.pullProcessTryNum){
			try {
				pullProcess = Runtime.getRuntime().exec("docker pull " + this.dockerImage);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			c ++;
		}
		try {
			ret = pullProcess.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.info("download image " + this.dockerImage + " end, code: " + ret);
		return (ret == 0);
	}

	public static void main(String[] args) {
		
		int result = -1;
		try {
			YarnDockerClient client = new YarnDockerClient();
			Runtime.getRuntime().addShutdownHook(new Thread(client.new ShutdownHook(), "shutdownWork"));
			LOG.info("Initializing Client");
			try {
				boolean doRun = client.init(args);
				if (!doRun) {
					System.exit(ExitCode.SUCC.getValue());
				}
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());
				System.exit(ExitCode.ILLEGAL_ARGUMENT.getValue());
			}catch(RuntimeException e1){
				System.err.println(e1.getLocalizedMessage());
				System.exit(ExitCode.RUNTIME_EXCEPTION.getValue());
			}
			result = client.runtask();
		} catch (Throwable t) {
			LOG.fatal("Error running CLient", t);
			System.exit(ExitCode.FAIL.getValue());
		}
		if (result == 0) {
			LOG.info("docker task completed successfully");
			System.exit(ExitCode.SUCC.getValue());
		}
		LOG.info("Application failed to complete successfully");
		LOG.info("client ends with value: " + result);
		System.exit(result);
	}

	public class WaitTaskRunner implements Runnable {
		private int exitcode = ExitCode.TIMEOUT.getValue();
		private DockerClient docker;
		private String id;

		public WaitTaskRunner(DockerClient docker, String id) {
			this.docker = docker;
			this.id = id;
		}

		@Override
		public void run() {
			WaitContainerCmd wc = docker.waitContainerCmd(id);
			try {
				exitcode = wc.exec();
				System.out.println("waitThread end");
			} catch (NotFoundException e) {
				e.printStackTrace();
			}

		}

		public int getExitCode() {
			return this.exitcode;
		}
		public void setExitCode(int value) {
			 this.exitcode = value;
		}
	}
	
	public  class ShutdownHook implements Runnable{

		public void run() {
			LOG.info("shutdownhook start");
			if(pullProcess != null){
				pullProcess.destroy();
			}
			
			if(containerId != null && !containerStoped){
				containerStoped = true;
				 StopContainerCmd stopContainerCmd = docker.stopContainerCmd(containerId);
				 stopContainerCmd.withTimeout(stopTimeout);
				try {
					stopContainerCmd.exec();
				} catch (Exception e) {
					
					LOG.info("docker container " + containerId
							+ " has been killed", e);
				}
				
				LOG.info("container  stoped by shutdownhook");
			}
			
			if(stdoutThread != null && stdoutThread.isAlive()){
				stdoutThread.interrupt();
			}
			if(stderrThread != null && stderrThread.isAlive()){
				stderrThread.interrupt();
			}
			if(waitThread != null && waitThread.isAlive()){
				waitThread.interrupt();
			}
			try {
				if(docker != null)
					docker.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			LOG.info("shutdownhook end");
		}
    	
    }
}
