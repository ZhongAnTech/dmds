/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.handler;

import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.manager.show.RollbackConfig;
import com.zhongan.dmds.manager.show.RollbackUser;
import com.zhongan.dmds.sqlParser.ManagerParseRollback;

public final class RollbackHandler {

  public static void handle(String stmt, ManagerConnection c, int offset) {
    switch (ManagerParseRollback.parse(stmt, offset)) {
      case ManagerParseRollback.CONFIG:
        RollbackConfig.execute(c);
        break;
      case ManagerParseRollback.ROUTE:
        c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        break;
      case ManagerParseRollback.USER:
        RollbackUser.execute(c);
        break;
      default:
        c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
    }
  }

}