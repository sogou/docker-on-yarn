package com.sogou.docker.service;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.sogou.docker.client.DockerApplicationSubmitter;
import com.sogou.docker.client.model.SubmitException;
import com.sogou.docker.util.StaticData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sogou.docker.client.model.SubmitItem;
import com.sogou.docker.client.model.SubmitReturnItem;

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
        try{
            sri = new DockerApplicationSubmitter(StaticData.DIS_SHELL_JAR).submitToYarn(si);
            status = Status.OK;
            message = sri.getMessage();
        }catch(SubmitException e){
            message = e.getMessage();
            status = Status.NO_CONTENT;
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


}
