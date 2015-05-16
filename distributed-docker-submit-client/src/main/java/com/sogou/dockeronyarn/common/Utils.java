package com.sogou.dockeronyarn.common;

/**
 * Created by guoshiwei on 15/5/11.
 */
public class Utils {

  public static void checkNotEmpty(String v, String msg) throws IllegalArgumentException{
    if(v == null || v.trim().isEmpty())
      throw new IllegalArgumentException(msg);
  }
}
