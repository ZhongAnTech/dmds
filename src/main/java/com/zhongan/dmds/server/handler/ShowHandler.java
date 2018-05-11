/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.handler;

import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.commons.parse.ServerParseShow;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.net.util.ShowFullTables;
import com.zhongan.dmds.net.util.ShowTables;
import com.zhongan.dmds.server.ServerConnection;
import com.zhongan.dmds.server.cmd.ShowDatabases;
import com.zhongan.dmds.server.cmd.ShowDmdsCluster;
import com.zhongan.dmds.server.cmd.ShowDmdsStatus;

public final class ShowHandler {

  public static void handle(String stmt, ServerConnection c, int offset) {

    // 排除 “ ` ” 符号
    stmt = StringUtil.replaceChars(stmt, "`", null);

    int type = ServerParseShow.parse(stmt, offset);
    switch (type) {
      case ServerParseShow.DATABASES:
        ShowDatabases.response(c);
        break;
      case ServerParseShow.TABLES:
        ShowTables.response(c, stmt, type);
        break;
      case ServerParseShow.FULLTABLES:
        ShowFullTables.response(c, stmt, type);
        break;
      case ServerParseShow.DMDS_STATUS:
        ShowDmdsStatus.response(c);
        break;
      case ServerParseShow.DMDS_CLUSTER:
        ShowDmdsCluster.response(c);
        break;
      default:
        c.execute(stmt, ServerParse.SHOW);
    }
  }

}