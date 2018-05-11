/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.handler;

import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.INIOProcessor;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.core.NIOConnection;
import com.zhongan.dmds.net.protocol.OkPacket;

public class KillHandler {

  public static void handle(String stmt, int offset, IServerConnection c) {
    String id = stmt.substring(offset).trim();
    if (StringUtil.isEmpty(id)) {
      c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "NULL connection id");
    } else {
      // get value
      long value = 0;
      try {
        value = Long.parseLong(id);
      } catch (NumberFormatException e) {
        c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Invalid connection id:" + id);
        return;
      }

      // kill myself
      if (value == c.getId()) {
        getOkPacket().write(c);
        c.write(c.allocate());
        return;
      }

      // get connection and close it
      NIOConnection fc = null;
      INIOProcessor[] processors = DmdsContext.getInstance().getProcessors();
      for (INIOProcessor p : processors) {
        if ((fc = p.getFrontends().get(value)) != null) {
          break;
        }
      }
      if (fc != null) {
        fc.close("killed");
        getOkPacket().write(c);
      } else {
        c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id:" + id);
      }
    }
  }

  private static OkPacket getOkPacket() {
    OkPacket packet = new OkPacket();
    packet.packetId = 1;
    packet.affectedRows = 0;
    packet.serverStatus = 2;
    return packet;
  }

}