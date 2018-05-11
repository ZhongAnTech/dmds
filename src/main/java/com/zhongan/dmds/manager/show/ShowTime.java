/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.util.LongUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;
import com.zhongan.dmds.sqlParser.ManagerParseShow;

import java.nio.ByteBuffer;

public final class ShowTime {

  private static final int FIELD_COUNT = 1;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("TIMESTAMP", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    eof.packetId = ++packetId;
  }

  public static void execute(ManagerConnection c, int type) {
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
    RowDataPacket row = getRow(type);
    row.packetId = ++packetId;
    buffer = row.write(buffer, c, true);

    // write last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // post write
    c.write(buffer);
  }

  private static RowDataPacket getRow(int type) {
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    switch (type) {
      case ManagerParseShow.TIME_CURRENT:
        row.add(LongUtil.toBytes(System.currentTimeMillis()));
        break;
      case ManagerParseShow.TIME_STARTUP:
        row.add(LongUtil.toBytes(DmdsContext.getInstance().getStartupTime()));
        break;
      default:
        row.add(LongUtil.toBytes(0L));
    }
    return row;
  }

}