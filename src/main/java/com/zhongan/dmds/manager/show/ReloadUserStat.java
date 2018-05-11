/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.protocol.OkPacket;
import com.zhongan.dmds.net.stat.UserStat;
import com.zhongan.dmds.net.stat.UserStatAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class ReloadUserStat {

  private static final Logger logger = LoggerFactory.getLogger(ReloadUserStat.class);

  public static void execute(ManagerConnection c) {

    Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
    for (UserStat userStat : statMap.values()) {
      userStat.reset();
    }

    StringBuilder s = new StringBuilder();
    s.append(c).append("Reset show @@sql  @@sql.sum  @@sql.slow success by manager");

    logger.warn(s.toString());

    OkPacket ok = new OkPacket();
    ok.packetId = 1;
    ok.affectedRows = 1;
    ok.serverStatus = 2;
    ok.message = "Reset show @@sql  @@sql.sum @@sql.slow success".getBytes();
    ok.write(c);
  }

}
