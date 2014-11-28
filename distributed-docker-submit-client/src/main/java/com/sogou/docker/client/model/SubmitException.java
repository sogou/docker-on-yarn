package com.sogou.docker.client.model;

/**
 * Created by guoshiwei on 14/11/28.
 */
public class SubmitException extends RuntimeException{
    public SubmitException(String message){
        super(message);
    }
    public SubmitException(String message, Exception e){
        super(message, e);
    }
}
