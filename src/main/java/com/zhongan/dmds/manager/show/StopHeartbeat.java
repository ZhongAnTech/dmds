/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.parse.Pair;
import com.zhongan.dmds.commons.util.FormatUtil;
import com.zhongan.dmds.commons.util.TimeUtil;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IPhysicalDBPool;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.protocol.OkPacket;
import com.zhongan.dmds.sqlParser.ManagerParseStop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 暂停数据节点心跳检测
 */
public final class StopHeartbeat {

  private static final Logger logger = LoggerFactory.getLogger(StopHeartbeat.class);
  ;

  public static void execute(String stmt, ManagerConnection c) {
    int count = 0;
    Pair<String[], Integer> keys = ManagerParseStop.getPair(stmt);
    if (keys.getKey() != null && keys.getValue() != null) {
      long time = keys.getValue().intValue() * 1000L;
      Map<String, IPhysicalDBPool> dns = DmdsContext.getInstance().getConfig().getDataHosts();
      for (String key : keys.getKey()) {
        IPhysicalDBPool dn = dns.get(key);
        if (dn != null) {
          dn.getSource().setHeartbeatRecoveryTime(TimeUtil.currentTimeMillis() + time);
          ++count;
          StringBuilder s = new StringBuilder();
          s.append(dn.getHostName()).append(" stop heartbeat '");
          logger.warn(s.append(FormatUtil.formatTime(time, 3)).append("' by manager.").toString());
        }
      }
    }
    OkPacket packet = new OkPacket();
    packet.packetId = 1;
    packet.affectedRows = count;
    packet.serverStatus = 2;
    packet.write(c);
  }

}