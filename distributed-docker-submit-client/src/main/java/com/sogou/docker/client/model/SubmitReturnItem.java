package com.sogou.docker.client.model;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SubmitReturnItem {
	private String appid = null;
    private ApplicationId applicationIdObject;

    public ApplicationId getApplicationIdObject() {
        return applicationIdObject;
    }

    public void setApplicationIdObject(ApplicationId applicationIdObject) {
        this.applicationIdObject = applicationIdObject;
    }

    private String apptrackingurl = null;
	private int status = 0;
	private String message = null;

	public SubmitReturnItem() {
		super();
	}

	public SubmitReturnItem(String appid, String apptrackingurl, int status, String message) {
		super();
		this.appid = appid;
		this.apptrackingurl = apptrackingurl;
		this.status = status;
		this.message  = message;
	}
	
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getAppid() {
		return appid;
	}

	public void setAppid(String appid) {
		this.appid = appid;
	}

	public String getApptrackingurl() {
		return apptrackingurl;
	}

	public void setApptrackingurl(String apptrackingurl) {
		this.apptrackingurl = apptrackingurl;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}


	
	
}
