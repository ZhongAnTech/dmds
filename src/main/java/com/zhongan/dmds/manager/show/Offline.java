/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.protocol.OkPacket;

public class Offline {

  private static final OkPacket ok = new OkPacket();

  static {
    ok.packetId = 1;
    ok.affectedRows = 1;
    ok.serverStatus = 2;
  }

  public static void execute(String stmt, ManagerConnection c) {
    DmdsContext.getInstance().offline();
    ok.write(c);
  }

}