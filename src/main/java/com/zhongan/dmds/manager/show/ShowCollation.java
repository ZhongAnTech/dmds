/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.util.IntegerUtil;
import com.zhongan.dmds.commons.util.LongUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.nio.ByteBuffer;

public final class ShowCollation {

  private static final int FIELD_COUNT = 6;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("COLLATION", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("CHARSET", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("DEFAULT", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("COMPILED", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("SORTLEN", Fields.FIELD_TYPE_LONG);
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
    RowDataPacket row = getRow(c.getCharset());
    row.packetId = ++packetId;
    buffer = row.write(buffer, c, true);

    // write lastEof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // write buffer
    c.write(buffer);
  }

  private static RowDataPacket getRow(String charset) {
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add("utf8_general_ci".getBytes());
    row.add("utf8".getBytes());
    row.add(IntegerUtil.toBytes(33));
    row.add("Yes".getBytes());
    row.add("Yes".getBytes());
    row.add(LongUtil.toBytes(1));
    return row;
  }

}