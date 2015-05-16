package com.sogou.dockeronyarn.util;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class StaticData {
	public static String LOG4J_CONFIG = "conf/log.properties";
	private static PropertyManager serverPropertyManager = new PropertyManager(
			"server.properties");

	/* server info */
	public static String SERVER_IP = serverPropertyManager
			.getProperty("server.ip");
	public static int SERVER_PORT = Integer.parseInt(serverPropertyManager
			.getProperty("server.port"));
	
	public static String DIS_SHELL_JAR = serverPropertyManager
			.getProperty("dis.shell.jar");
	
	



	public static <T> Logger getLogger(Class<T> clazz) {
		PropertyConfigurator.configure(clazz.getClassLoader().getResource(
				StaticData.LOG4J_CONFIG));
		return Logger.getLogger(clazz.getName());
	}
}
