/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IDmdsConfig;
import com.zhongan.dmds.core.IPhysicalDBNode;
import com.zhongan.dmds.core.IPhysicalDBPool;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.protocol.OkPacket;

public class ClearSlow {

  public static void dataNode(ManagerConnection c, String name) {
    IPhysicalDBNode dn = DmdsContext.getInstance().getConfig().getDataNodes().get(name);
    IPhysicalDBPool ds = null;
    if (dn != null && ((ds = dn.getDbPool()) != null)) {
      c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
    } else {
      c.writeErrMessage(ErrorCode.ER_YES, "Invalid DataNode:" + name);
    }
  }

  public static void schema(ManagerConnection c, String name) {
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    SchemaConfig schema = conf.getSchemas().get(name);
    if (schema != null) {
      c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
    } else {
      c.writeErrMessage(ErrorCode.ER_YES, "Invalid Schema:" + name);
    }
  }

}