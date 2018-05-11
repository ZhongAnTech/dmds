/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.handler;

import com.zhongan.dmds.core.IServerConnection;

public final class BeginHandler {

  private static final byte[] AC_OFF = new byte[]{7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};

  public static void handle(String stmt, IServerConnection c) {
    if (c.isAutocommit()) {
      c.setAutocommit(false);
      c.write(c.writeToBuffer(AC_OFF, c.allocate()));
    } else {
      c.getSession2().commit();
    }
  }

}