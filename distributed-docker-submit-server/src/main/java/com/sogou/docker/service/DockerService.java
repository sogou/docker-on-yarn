package com.sogou.docker.service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.log4j.Logger;

import com.sogou.docker.client.DockerClient;
import com.sogou.docker.model.SubmitItem;
import com.sogou.docker.model.SubmitReturnItem;
import com.sogou.docker.util.StaticData;


@Path("/docker")
public class DockerService {
	private static final Log logger = LogFactory.getLog(DockerService.class);
	
	@POST
	@Path("submit")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response submit(SubmitItem si) {
		logger.debug("submit service start");
		Status status = Status.NO_CONTENT;
		String message = null;
		SubmitReturnItem sri = null;
		if (si.getCommand() == null || si.getWorkingdir() == null || si.getDocker_image() == null) {
			message = "field command, workingdir and docker_image are needed";
			// return
		} else {
			String shellArgs = "";
			shellArgs += "\" -timeout " +  si.getTimeout();
			shellArgs += " -workingdir " +  si.getWorkingdir();
			String[] parts = si.getCommand().split("\\s{1,}");
			for(int i = 0 ; i< parts.length; ++i){
				if(parts[i] == null) continue;
				shellArgs += " -cmd_args " +  parts[i] + "";
			}
			
			shellArgs += " -container_memory " +  si.getContainer_memory();
			shellArgs += " -container_vcores " +  si.getContainer_vcores();
			shellArgs += " -docker_image " +  si.getDocker_image();
			if(si.isDebug()){
				shellArgs += " -debug ";
			}
			if(si.getVirualdirs() != null){
				for(int i = 0; i < si.getVirualdirs().length; ++i){
					if(si.getVirualdirs()[i] == null || si.getVirualdirs()[i].trim().length() == 0) continue;
					shellArgs += " -virtualdir " +  si.getVirualdirs()[i];
				}
				
			}
			shellArgs +="\"";
			String[] args = {"-jar", StaticData.DIS_SHELL_JAR
					 ,"-num_containers", "1", "-container_memory", String.valueOf(si.getContainer_memory()), 
					 "-master_memory",String.valueOf(si.getMaster_memory()), "-container_retry", String.valueOf(si.getContainer_retry()) 
					 ,"-priority", String.valueOf(si.getPriority()), "-appname", si.getAppname(), "-timeout", String.valueOf(si.getTimeout())
					 ,"-queue",si.getQueue()
					 , "-shell_args", shellArgs};
				sri = DockerService.service(args);
				status = Status.OK;
				message = sri.getMessage();
				
		}

		logger.debug(message);

		return Response.status(status).header("Message", message).entity(sri).build();
	}
	
	@GET
	@Path("test")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response test(@QueryParam("str") String str) {
		logger.debug("test service start");
		Status status = Status.NO_CONTENT;
		String message = null;

		
				status = Status.OK;
				message = "submit succeed";

		logger.debug(message);

		return Response.status(status).header("Message", message).entity(str).build();
	}
	
	  public static SubmitReturnItem service(String[] args) {
		    boolean result = false;
		    SubmitReturnItem ret = new SubmitReturnItem();
		    ApplicationId appid  = null;
		    ApplicationReport report = null;
		    try {
		      DockerClient client = new DockerClient();
		      logger.info("Initializing Client");
		      try {
		        boolean doRun = client.init(args);
		        if (!doRun) {
		        	ret.setMessage("no need to run, just help");
		          return ret;
		        }
		      } catch (IllegalArgumentException e) {
		        System.err.println(e.getLocalizedMessage());
		        client.printUsage();
		        ret.setMessage(e.getLocalizedMessage());
		        return ret;
		      }
		       appid = client.run();
		        report = client.getYarnClient().getApplicationReport(appid);
		      
		    } catch (Throwable t) {
		    	logger.fatal("Error running CLient", t);
		      ret.setMessage(t.getLocalizedMessage());
		        return ret;
		    }
		    
		    if (appid != null) {
		    	logger.info("Application complete submit successfully");
		     
		     ret.setAppid(appid.toString());
		     ret.setApptrackingurl(report.getTrackingUrl());
		     return ret;
		    } 
		    return ret;
		  }

	
}
