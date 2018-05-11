/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.cmd;

import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.*;

import java.nio.ByteBuffer;

/**
 * 加入了offline状态推送，用于心跳语句。
 * Base on ShowMyCatStatus
 */
public class ShowDmdsStatus {

  private static final int FIELD_COUNT = 1;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();
  private static final RowDataPacket status = new RowDataPacket(FIELD_COUNT);
  private static final EOFPacket lastEof = new EOFPacket();
  private static final ErrorPacket error = PacketUtil.getShutdown();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;
    fields[i] = PacketUtil.getField("STATUS", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;
    eof.packetId = ++packetId;
    status.add("ON".getBytes());
    status.packetId = ++packetId;
    lastEof.packetId = ++packetId;
  }

  public static void response(IServerConnection c) {
    if (DmdsContext.getInstance().isOnline()) {
      ByteBuffer buffer = c.allocate();
      buffer = header.write(buffer, c, true);
      for (FieldPacket field : fields) {
        buffer = field.write(buffer, c, true);
      }
      buffer = eof.write(buffer, c, true);
      buffer = status.write(buffer, c, true);
      buffer = lastEof.write(buffer, c, true);
      c.write(buffer);
    } else {
      error.write(c);
    }
  }

}