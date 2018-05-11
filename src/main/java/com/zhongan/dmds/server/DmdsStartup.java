/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server;

import com.zhongan.dmds.config.model.SystemConfig;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Base on MycatStartup
 * remove support of zk
 */
public final class DmdsStartup {

  private static final String dateFormat = "yyyy-MM-dd HH:mm:ss";

  public static void main(String[] args) {
    try {
      String home = SystemConfig.getHomePath();
      if (home == null) {
        System.err.println(SystemConfig.SYS_HOME + "is not set");
        System.exit(-1);
      }
      DmdsServer server = DmdsServer.getInstance();
      server.startup();
      System.out.println("DMDS Server startup successfully. see logs in logs/dmds.log");
      while (true) {
        Thread.sleep(300 * 1000);
      }
    } catch (Throwable e) {
      e.printStackTrace();
      SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
      LoggerFactory.getLogger(DmdsStartup.class).error(sdf.format(new Date()) + " startup error", e);
      System.exit(-1);
    }
  }
}
