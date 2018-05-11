/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.handler;

import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.manager.show.SelectSessionAutoIncrement;
import com.zhongan.dmds.manager.show.SelectTxReadOnly;
import com.zhongan.dmds.manager.show.SelectVersionComment;
import com.zhongan.dmds.sqlParser.ManagerParseSelect;

import static com.zhongan.dmds.sqlParser.ManagerParseSelect.*;

public final class SelectHandler {

  public static void handle(String stmt, ManagerConnection c, int offset) {
    switch (ManagerParseSelect.parse(stmt, offset)) {
      case VERSION_COMMENT:
        SelectVersionComment.execute(c);
        break;
      case SESSION_AUTO_INCREMENT:
        SelectSessionAutoIncrement.execute(c);
        break;
      case SESSION_TX_READ_ONLY:
        SelectTxReadOnly.response(c);
        break;
      default:
        c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
    }
  }

}