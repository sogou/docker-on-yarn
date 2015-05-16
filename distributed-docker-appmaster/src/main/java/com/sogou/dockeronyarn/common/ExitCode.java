package com.sogou.dockeronyarn.common;

public enum ExitCode{
	
	CONTAINER_NOT_CREATE(254), ILLEGAL_ARGUMENT(253), FAIL(1), SUCC(0), TIMEOUT(250), KILLED(255);
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




