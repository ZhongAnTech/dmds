/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.parse.Pair;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IPhysicalDBPool;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.protocol.OkPacket;
import com.zhongan.dmds.sqlParser.ManagerParseSwitch;

import java.util.Map;

/**
 * 切换数据节点的数据源
 */
public final class SwitchDataSource {

  public static void response(String stmt, ManagerConnection c) {
    int count = 0;
    Pair<String[], Integer> pair = ManagerParseSwitch.getPair(stmt);
    Map<String, IPhysicalDBPool> dns = DmdsContext.getInstance().getConfig().getDataHosts();
    Integer idx = pair.getValue();
    for (String key : pair.getKey()) {
      IPhysicalDBPool dn = dns.get(key);
      if (dn != null) {
        int m = dn.getActivedIndex();
        int n = (idx == null) ? dn.next(m) : idx.intValue();
        if (dn.switchSource(n, false, "MANAGER")) {
          ++count;
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