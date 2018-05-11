/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.handler;

import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.commons.parse.ServerParseStart;
import com.zhongan.dmds.server.ServerConnection;

public final class StartHandler {

  private static final byte[] AC_OFF = new byte[]{7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};

  public static void handle(String stmt, ServerConnection c, int offset) {
    switch (ServerParseStart.parse(stmt, offset)) {
      case ServerParseStart.TRANSACTION:
        if (c.isAutocommit()) {
          c.setAutocommit(false);
          c.write(c.writeToBuffer(AC_OFF, c.allocate()));
        } else {
          c.getSession2().commit();
        }
        break;
      default:
        c.execute(stmt, ServerParse.START);
    }
  }

}