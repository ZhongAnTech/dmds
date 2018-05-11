/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.cmd;

import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.net.frontend.FrontendConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.ErrorPacket;
import com.zhongan.dmds.net.protocol.OkPacket;

/**
 * 加入了offline状态推送，用于心跳语句。
 */
public class Ping {

  private static final ErrorPacket error = PacketUtil.getShutdown();

  public static void response(FrontendConnection c) {
    if (DmdsContext.getInstance().isOnline()) {
      c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
    } else {
      error.write(c);
    }
  }

}