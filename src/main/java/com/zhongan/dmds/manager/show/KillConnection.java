/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.util.SplitUtil;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.INIOProcessor;
import com.zhongan.dmds.core.NIOConnection;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.frontend.FrontendConnection;
import com.zhongan.dmds.net.protocol.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class KillConnection {

  private static final Logger logger = LoggerFactory.getLogger(KillConnection.class);

  public static void response(String stmt, int offset, ManagerConnection mc) {
    int count = 0;
    List<FrontendConnection> list = getList(stmt, offset, mc);
    if (list != null) {
      for (NIOConnection c : list) {
        StringBuilder s = new StringBuilder();
        logger.warn(s.append(c).append("killed by manager").toString());
        c.close("kill by manager");
        count++;
      }
    }
    OkPacket packet = new OkPacket();
    packet.packetId = 1;
    packet.affectedRows = count;
    packet.serverStatus = 2;
    packet.write(mc);
  }

  private static List<FrontendConnection> getList(String stmt, int offset, ManagerConnection mc) {
    String ids = stmt.substring(offset).trim();
    if (ids.length() > 0) {
      String[] idList = SplitUtil.split(ids, ',', true);
      List<FrontendConnection> fcList = new ArrayList<FrontendConnection>(idList.length);
      INIOProcessor[] processors = DmdsContext.getInstance().getProcessors();
      for (String id : idList) {
        long value = 0;
        try {
          value = Long.parseLong(id);
        } catch (NumberFormatException e) {
          continue;
        }
        FrontendConnection fc = null;
        for (INIOProcessor p : processors) {
          if ((fc = (FrontendConnection) p.getFrontends().get(value)) != null) {
            fcList.add(fc);
            break;
          }
        }
      }
      return fcList;
    }
    return null;
  }

}