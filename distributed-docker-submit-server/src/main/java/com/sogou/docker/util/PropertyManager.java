package com.sogou.docker.util;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class PropertyManager {
	private String propertyFile;
	private Configuration config;

	public PropertyManager(String propertyFile) {
		this.propertyFile = propertyFile;
		try {
			config = new PropertiesConfiguration(PropertyManager.class
					.getClassLoader().getResource(propertyFile));
		} catch (ConfigurationException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public String getProperty(String key) {
		if (config.containsKey(key)) {
			return config.getString(key);
		} else {
			return null;
		}
	}

	public String getPropertyFile() {
		return propertyFile;
	}

	public void setPropertyFile(String propertyFile) {
		this.propertyFile = propertyFile;
	}
}
