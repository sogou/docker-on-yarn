package com.sogou.docker.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.sogou.docker.util.StaticData;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.core.reflection.ReflectionHelper;
import com.sun.jersey.spi.container.servlet.ServletContainer;

public class DockerServer {
	
	public static String HTTP_CONTEXT = "/";
	private static StatisticsHandler STATIC = null;
	
	private static URI BASE_URI = UriBuilder
			.fromUri(String.format("http://%s/", StaticData.SERVER_IP))
			.port(StaticData.SERVER_PORT).build();


	public void startHttpService() throws Exception {
		
		try {
			ResourceConfig rc = new PackagesResourceConfig("com.sogou.docker.service");
			HttpServer httpServer = GrizzlyServerFactory.createHttpServer(
					BASE_URI, rc);
			httpServer.start();
			
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	public static StatisticsHandler getStaticHandler() {
		return STATIC;
	}

}
