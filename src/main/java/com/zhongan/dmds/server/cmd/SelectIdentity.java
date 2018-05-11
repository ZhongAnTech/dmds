/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.cmd;

import com.zhongan.dmds.commons.parse.ParseUtil;
import com.zhongan.dmds.commons.util.LongUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.nio.ByteBuffer;

public class SelectIdentity {

  private static final int FIELD_COUNT = 1;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);

  static {
    byte packetId = 0;
    header.packetId = ++packetId;
  }

  public static void response(IServerConnection c, String stmt, int aliasIndex,
      final String orgName) {
    String alias = ParseUtil.parseAlias(stmt, aliasIndex);
    if (alias == null) {
      alias = orgName;
    }

    ByteBuffer buffer = c.allocate();

    // write header
    buffer = header.write(buffer, c, true);

    // write fields
    byte packetId = header.packetId;
    FieldPacket field = PacketUtil.getField(alias, orgName, Fields.FIELD_TYPE_LONGLONG);
    field.packetId = ++packetId;
    buffer = field.write(buffer, c, true);

    // write eof
    EOFPacket eof = new EOFPacket();
    eof.packetId = ++packetId;
    buffer = eof.write(buffer, c, true);

    // write rows
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(LongUtil.toBytes(c.getLastInsertId()));
    row.packetId = ++packetId;
    buffer = row.write(buffer, c, true);

    // write last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // post write
    c.write(buffer);
  }

}