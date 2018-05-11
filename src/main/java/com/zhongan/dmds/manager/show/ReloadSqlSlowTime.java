/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.protocol.OkPacket;
import com.zhongan.dmds.net.stat.UserStatAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadSqlSlowTime {

  private static final Logger logger = LoggerFactory.getLogger(ReloadSqlSlowTime.class);

  public static void execute(ManagerConnection c, long time) {

    //Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
    UserStatAnalyzer.getInstance().setSlowTime(time);
    // for (UserStat userStat : statMap.values()) {
    // 	userStat.setSlowTime(time);
    // }

    StringBuilder s = new StringBuilder();
    s.append(c).append("Reset show  @@sql.slow=" + time + " time success by manager");

    logger.warn(s.toString());

    OkPacket ok = new OkPacket();
    ok.packetId = 1;
    ok.affectedRows = 1;
    ok.serverStatus = 2;
    ok.message = "Reset show  @@sql.slow time success".getBytes();
    ok.write(c);
    System.out.println(s.toString());
  }

}