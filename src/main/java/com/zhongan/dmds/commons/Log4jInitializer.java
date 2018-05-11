/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons;

//import org.apache.log4j.LogManager;
//import org.apache.log4j.helpers.FileWatchdog;
//import org.apache.log4j.xml.DOMConfigurator;

/**
 * Use logback
 */
public final class Log4jInitializer {

  private static final String format = "yyyy-MM-dd HH:mm:ss";

//	public static void configureAndWatch(String filename, long delay) {
//		XMLWatchdog xdog = new XMLWatchdog(filename);
//		xdog.setName("Log4jWatchdog");
//		xdog.setDelay(delay);
//		xdog.start();
//	}
//
//	private static final class XMLWatchdog extends FileWatchdog {
//		public XMLWatchdog(String filename) {
//			super(filename);
//		}
//
//		@Override
//		public void doOnChange() {
//			new DOMConfigurator().doConfigure(filename, LogManager.getLoggerRepository());
//			System.out.println(
//					"log4j " + new SimpleDateFormat(format).format(new Date()) + " [" + filename + "] load completed.");
//		}
//	}

}