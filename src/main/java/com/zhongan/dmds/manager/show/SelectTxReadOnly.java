/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.util.LongUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.net.frontend.FrontendConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.nio.ByteBuffer;

public class SelectTxReadOnly {

  private static final int FIELD_COUNT = 1;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();
  private static byte[] longbt = LongUtil.toBytes(0);

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;
    fields[i] = PacketUtil.getField("@@session.tx_read_only", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;
    eof.packetId = ++packetId;

  }

  public static void response(FrontendConnection c) {
    ByteBuffer buffer = c.allocate();
    buffer = header.write(buffer, c, true);
    for (FieldPacket field : fields) {
      buffer = field.write(buffer, c, true);
    }
    buffer = eof.write(buffer, c, true);
    byte packetId = eof.packetId;
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(longbt);
    row.packetId = ++packetId;
    buffer = row.write(buffer, c, true);
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);
    c.write(buffer);
  }

}
