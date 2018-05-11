/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.handler;

import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.manager.show.ClearSlow;
import com.zhongan.dmds.sqlParser.ManagerParseClear;

public class ClearHandler {

  public static void handle(String stmt, ManagerConnection c, int offset) {
    int rs = ManagerParseClear.parse(stmt, offset);
    switch (rs & 0xff) {
      case ManagerParseClear.SLOW_DATANODE: {
        String name = stmt.substring(rs >>> 8).trim();
        if (StringUtil.isEmpty(name)) {
          c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        } else {
          ClearSlow.dataNode(c, name);
        }
        break;
      }
      case ManagerParseClear.SLOW_SCHEMA: {
        String name = stmt.substring(rs >>> 8).trim();
        if (StringUtil.isEmpty(name)) {
          c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        } else {
          ClearSlow.schema(c, name);
        }
        break;
      }
      default:
        c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
    }
  }
}