package com.sogou.dockeronyarn.util;

import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.log4j.Logger;
import com.sogou.util.sendalert.bean.AlertLevel;
import com.sogou.util.sendalert.bean.AlertType;


public class LogUtil {
	private static Logger logger = Logger.getLogger(LogUtil.class);
	private static ConcurrentSkipListMap<Long, Exception> exceptions = new ConcurrentSkipListMap<Long, Exception>();
	//private static AlertSenderThread alertSender = null;
	
//	static {
//		alertSender = new AlertSenderThread();
//		alertSender.start();
//	}
	
	public static void debug(String info) {
		logger.debug(info);
	}
	
	public static void debug(Object ...infos) {
		if (!logger.isDebugEnabled()) {
			return ;
		}
		logger.debug(objsToString(infos));
	}
	
	public static void log(String info) {
		logger.info(info);
	}
	
	public static void log(Object ...infos) {
		if (!logger.isInfoEnabled()) {
			return ;
		}
		logger.info(objsToString(infos));
	}
	
	public static void warn(String info) {
		logger.warn(info);
	}
	
	public static void warn(Object ...infos) {
		String logInfo = objsToString(infos);
		logger.warn(logInfo);
	}
	
	private static String objsToString(Object ...infos) {
		StringBuilder logInfo = new StringBuilder();
		for (Object info : infos) {
			if (info == null) {
				logInfo.append("null");
			} else {
				logInfo.append(info.toString());
			}
			logInfo.append(' ');
		}
		return logInfo.toString();
	}

	public static void error(String info) {
		logger.error(info);
	}
	
	public static void fatal(String info) {
		logger.fatal(info);
	}
	
	public static void error(Exception e) {
		exceptions.put(System.currentTimeMillis(), e);
		if (exceptions.size() > 20) {
			exceptions.remove(exceptions.firstKey());
		}
		logger.error("Exception:", e);
	}
	
	public static void error(String info, Exception e) {
		exceptions.put(System.currentTimeMillis(), e);
		if (exceptions.size() > 20) {
			exceptions.remove(exceptions.firstKey());
		}
		logger.error(info, e);
	}

	public static ConcurrentSkipListMap<Long, Exception> getExceptions() {
		return exceptions;
	}

	public static void setExceptions(ConcurrentSkipListMap<Long, Exception> exceptions) {
		LogUtil.exceptions = exceptions;
	}

	public static void warnAndSendAlert(String title, Object ...infos) { 
		String logInfo = objsToString(infos);
		logger.warn(logInfo);
	}

	private static void sendAlert(String title, String logInfo, AlertType type, AlertLevel level) { 
		//alertSender.addAlertInfo(new AlertInfo(title, logInfo, type, level)); 
	}

	public static void errorAndSendAlert(String title, Object ...infos) {
		String logInfo = objsToString(infos);
		logger.warn(logInfo);
	}

	public static void sendNormalAlert(String title, String info) {
//		sendAlert(title, info, AlertType.DAILY, AlertLevel.NORMAL);
	}

	public static void fatal(Exception e) {
		logger.fatal(e);
		e.printStackTrace();
	}

	public static void error(Object ...infos) {
		String logInfo = objsToString(infos);
		logger.error(logInfo);
	}
	
}

class AlertInfo {
	
	String title = null;
	String logInfo = null;
	AlertType type = null;
	AlertLevel level = null;
	
	public AlertInfo(String title, String logInfo, AlertType type,
			AlertLevel level) {
		super();
		this.title = title;
		this.logInfo = logInfo;
		this.type = type;
		this.level = level;
	}
	
}

