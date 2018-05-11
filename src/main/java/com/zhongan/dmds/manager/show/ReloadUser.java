/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.protocol.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReloadUser {

  private static final Logger logger = LoggerFactory.getLogger(ReloadUser.class);

  public static void execute(ManagerConnection c) {
    boolean status = false;
    if (status) {
      StringBuilder s = new StringBuilder();
      s.append(c).append("Reload userConfig success by manager");
      logger.warn(s.toString());
      OkPacket ok = new OkPacket();
      ok.packetId = 1;
      ok.affectedRows = 1;
      ok.serverStatus = 2;
      ok.message = "Reload userConfig success".getBytes();
      ok.write(c);
    } else {
      c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
    }
  }

}