/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.protocol.OkPacket;
import com.zhongan.dmds.net.stat.QueryResultDispatcher;

/**
 * 关闭/打开  统计模块
 * <p>
 * reload @@sqlstat=close; reload @@sqlstat=open;
 */
public class ReloadSqlStat {

  public static void execute(ManagerConnection c, String stat) {

    boolean isOk = false;

    if (stat != null) {

      if (stat.equalsIgnoreCase("OPEN")) {
        isOk = QueryResultDispatcher.open();

      } else if (stat.equalsIgnoreCase("CLOSE")) {
        isOk = QueryResultDispatcher.close();
      }

      StringBuffer sBuffer = new StringBuffer(35);
      sBuffer.append("Set sql stat module isclosed=").append(stat).append(",");
      sBuffer.append((isOk == true ? " to succeed" : " to fail"));
      sBuffer.append(" by manager. ");

      OkPacket ok = new OkPacket();
      ok.packetId = 1;
      ok.affectedRows = 1;
      ok.serverStatus = 2;
      ok.message = sBuffer.toString().getBytes();
      ok.write(c);
    }
  }


}
