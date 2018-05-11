/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.cmd;

import com.zhongan.dmds.commons.util.TimeUtil;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.net.protocol.ErrorPacket;
import com.zhongan.dmds.net.protocol.HeartbeatPacket;
import com.zhongan.dmds.net.protocol.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Heartbeat {

  private static final Logger HEARTBEAT = LoggerFactory.getLogger(Heartbeat.class);

  public static void response(IServerConnection c, byte[] data) {
    HeartbeatPacket hp = new HeartbeatPacket();
    hp.read(data);
    if (DmdsContext.getInstance().isOnline()) {
      OkPacket ok = new OkPacket();
      ok.packetId = 1;
      ok.affectedRows = hp.id;
      ok.serverStatus = 2;
      ok.write(c);
      if (HEARTBEAT.isInfoEnabled()) {
        HEARTBEAT.info(responseMessage("OK", c, hp.id));
      }
    } else {
      ErrorPacket error = new ErrorPacket();
      error.packetId = 1;
      error.errno = ErrorCode.ER_SERVER_SHUTDOWN;
      error.message = String.valueOf(hp.id).getBytes();
      error.write(c);
      if (HEARTBEAT.isInfoEnabled()) {
        HEARTBEAT.info(responseMessage("ERROR", c, hp.id));
      }
    }
  }

  private static String responseMessage(String action, IServerConnection c, long id) {
    return new StringBuilder("RESPONSE:").append(action).append(", id=").append(id)
        .append(", host=")
        .append(c.getHost()).append(", port=").append(c.getPort()).append(", time=")
        .append(TimeUtil.currentTimeMillis()).toString();
  }

}