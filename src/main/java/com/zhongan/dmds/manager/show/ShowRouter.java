/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.INIOProcessor;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public final class ShowRouter {

  private static final int FIELD_COUNT = 5;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("PROCESSOR_NAME", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("ROUTE_COUNT", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("TIME_COUNT", Fields.FIELD_TYPE_FLOAT);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("MAX_ROUTE_TIME", Fields.FIELD_TYPE_FLOAT);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("MAX_ROUTE_SQL_ID", Fields.FIELD_TYPE_LONGLONG);
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
    for (INIOProcessor p : DmdsContext.getInstance().getProcessors()) {
      RowDataPacket row = getRow(p, c.getCharset());
      row.packetId = ++packetId;
      buffer = row.write(buffer, c, true);
    }

    // write last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // write buffer
    c.write(buffer);
  }

  private static final NumberFormat nf = DecimalFormat.getInstance();

  static {
    nf.setMaximumFractionDigits(3);
  }

  private static RowDataPacket getRow(INIOProcessor processor, String charset) {
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(processor.getName().getBytes());
    row.add(null);
    row.add(null);
    row.add(null);
    row.add(null);
    return row;
  }

}