/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.handler;

import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.manager.show.SwitchDataSource;
import com.zhongan.dmds.sqlParser.ManagerParseSwitch;

import static com.zhongan.dmds.sqlParser.ManagerParseSwitch.DATASOURCE;

public final class SwitchHandler {

  public static void handler(String stmt, ManagerConnection c, int offset) {
    switch (ManagerParseSwitch.parse(stmt, offset)) {
      case DATASOURCE:
        SwitchDataSource.response(stmt, c);
        break;
      default:
        c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
    }
  }

}