/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.util.LongUtil;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * 查询指定SQL在各个pool中的执行情况
 */
public final class ShowSQLDetail {

  private static final NumberFormat nf = DecimalFormat.getInstance();
  private static final int FIELD_COUNT = 5;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    nf.setMaximumFractionDigits(3);

    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("DATA_SOURCE", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("EXECUTE", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("TIME", Fields.FIELD_TYPE_DOUBLE);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("LAST_EXECUTE_TIMESTAMP", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_DOUBLE);
    fields[i++].packetId = ++packetId;

    eof.packetId = ++packetId;
  }

  public static void execute(ManagerConnection c, long sql) {
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
    for (int i = 0; i < 3; i++) {
      RowDataPacket row = getRow(sql, c.getCharset());
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

  private static RowDataPacket getRow(long sql, String charset) {
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add("mysql_1".getBytes());
    row.add(LongUtil.toBytes(123L));
    row.add(StringUtil.encode(nf.format(2.3), charset));
    row.add(LongUtil.toBytes(1279188420682L));
    row.add(StringUtil.encode(nf.format(3.42), charset));
    return row;
  }

}