/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.stat.QueryConditionAnalyzer;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.protocol.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadQueryCf {

  private static final Logger logger = LoggerFactory.getLogger(ReloadSqlSlowTime.class);

  public static void execute(ManagerConnection c, String cf) {

    if (cf == null) {
      cf = "NULL";
    }

    QueryConditionAnalyzer.getInstance().setCf(cf);

    StringBuilder s = new StringBuilder();
    s.append(c).append("Reset show  @@sql.condition=" + cf + " success by manager");

    OkPacket ok = new OkPacket();
    ok.packetId = 1;
    ok.affectedRows = 1;
    ok.serverStatus = 2;
    ok.message = "Reset show  @@sql.condition success".getBytes();
    ok.write(c);
    System.out.println(s.toString());
  }

}
