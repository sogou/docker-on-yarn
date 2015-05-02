package com.sogou.docker.client.docker;

public enum ExitCode{
	
	CONTAINER_NOT_CREATE(254), ILLEGAL_ARGUMENT(253), FAIL(1), SUCC(0), TIMEOUT(250), KILLED(255), IMAGE_NOTFOUND(252), RUNTIME_EXCEPTION(251);
	
	private int value;
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	private ExitCode(int value){
		this.value = value;
	}
	
}




