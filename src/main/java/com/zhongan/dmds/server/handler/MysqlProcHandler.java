/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.handler;

import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;

import java.nio.ByteBuffer;

public class MysqlProcHandler {

  private static final int FIELD_COUNT = 2;
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];

  static {
    fields[0] = PacketUtil.getField("name", Fields.FIELD_TYPE_VAR_STRING);
    fields[1] = PacketUtil.getField("type", Fields.FIELD_TYPE_VAR_STRING);
  }

  public static void handle(String stmt, IServerConnection c) {

    ByteBuffer buffer = c.allocate();

    // write header
    ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    byte packetId = header.packetId;
    buffer = header.write(buffer, c, true);

    // write fields
    for (FieldPacket field : fields) {
      field.packetId = ++packetId;
      buffer = field.write(buffer, c, true);
    }

    // write eof
    EOFPacket eof = new EOFPacket();
    eof.packetId = ++packetId;
    buffer = eof.write(buffer, c, true);

    // write last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // post write
    c.write(buffer);

  }

}