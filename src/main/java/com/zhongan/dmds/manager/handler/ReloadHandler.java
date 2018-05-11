/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.handler;

import com.zhongan.dmds.commons.parse.ParseUtil;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.manager.show.*;
import com.zhongan.dmds.sqlParser.ManagerParseReload;

public final class ReloadHandler {

  public static void handle(String stmt, ManagerConnection c, int offset) {
    int rs = ManagerParseReload.parse(stmt, offset);
    switch (rs) {
      case ManagerParseReload.CONFIG:
        ReloadConfig.execute(c, false);
        break;
      case ManagerParseReload.CONFIG_ALL:
        ReloadConfig.execute(c, true);
        break;
      case ManagerParseReload.ROUTE:
        c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        break;
      case ManagerParseReload.USER:
        ReloadUser.execute(c);
        break;
      case ManagerParseReload.USER_STAT:
        ReloadUserStat.execute(c);
        break;
      case ManagerParseReload.SQL_SLOW:
        ReloadSqlSlowTime.execute(c, ParseUtil.getSQLId(stmt));
        break;
      case ManagerParseReload.SQL_STAT:
        String stat = ParseUtil.parseString(stmt);
        ReloadSqlStat.execute(c, stat);
        break;
      case ManagerParseReload.QUERY_CF:
        String filted = ParseUtil.parseString(stmt);
        ReloadQueryCf.execute(c, filted);
        break;
      default:
        c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
    }
  }

}