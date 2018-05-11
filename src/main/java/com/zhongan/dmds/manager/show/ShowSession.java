/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.core.*;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * show front session detail info
 */
public class ShowSession {

  private static final int FIELD_COUNT = 3;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("SESSION", Fields.FIELD_TYPE_VARCHAR);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("DN_COUNT", Fields.FIELD_TYPE_VARCHAR);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("DN_LIST", Fields.FIELD_TYPE_VARCHAR);
    fields[i++].packetId = ++packetId;
    eof.packetId = ++packetId;
  }

  public static void execute(ManagerConnection c) {
    ByteBuffer buffer = c.allocate();

    // write header
    buffer = header.write(buffer, c, true);

    // write fields
    for (FieldPacket field : fields) {
      buffer = field.write(buffer, c, true);
    }

    // write eof
    buffer = eof.write(buffer, c, true);

    // write rows
    byte packetId = eof.packetId;
    for (INIOProcessor process : DmdsContext.getInstance().getProcessors()) {
      for (NIOConnection front : process.getFrontends().values()) {

        if (!(front instanceof IServerConnection)) {
          continue;
        }
        IServerConnection sc = (IServerConnection) front;
        RowDataPacket row = getRow(sc, c.getCharset());
        if (row != null) {
          row.packetId = ++packetId;
          buffer = row.write(buffer, c, true);
        }
      }
    }

    // write last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // write buffer
    c.write(buffer);
  }

  private static RowDataPacket getRow(IServerConnection sc, String charset) {
    StringBuilder sb = new StringBuilder();
    Session ssesion = sc.getSession2();
    Collection<BackendConnection> backConnections = ssesion.getTargetMap().values();
    int cncount = backConnections.size();
    if (cncount == 0) {
      return null;
    }
    for (BackendConnection backCon : backConnections) {
      sb.append(backCon).append("\r\n");
    }
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(StringUtil.encode(sc.getId() + "", charset));
    row.add(StringUtil.encode(cncount + "", charset));
    row.add(StringUtil.encode(sb.toString(), charset));
    return row;
  }
}
