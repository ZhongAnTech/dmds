/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.util.IntegerUtil;
import com.zhongan.dmds.commons.util.LongUtil;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.commons.util.TimeUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.INIOProcessor;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.core.NIOConnection;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.frontend.FrontendConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.nio.ByteBuffer;

/**
 * 查看当前有效连接信息
 */
public final class ShowConnection {

  private static final int FIELD_COUNT = 15;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("PROCESSOR",
        Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("LOCAL_PORT", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil
        .getField("CHARSET", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("ALIVE_TIME(S)",
        Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("RECV_BUFFER", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("SEND_QUEUE", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil
        .getField("txlevel", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("autocommit",
        Fields.FIELD_TYPE_VAR_STRING);
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
    String charset = c.getCharset();
    INIOProcessor[] processors = DmdsContext.getInstance().getProcessors();
    for (INIOProcessor p : processors) {
      for (NIOConnection fc : p.getFrontends().values()) {
        if (fc != null) {
          RowDataPacket row = getRow((FrontendConnection) fc, charset);
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

  private static RowDataPacket getRow(FrontendConnection c, String charset) {
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(c.getProcessor().getName().getBytes());
    row.add(LongUtil.toBytes(c.getId()));
    row.add(StringUtil.encode(c.getHost(), charset));
    row.add(IntegerUtil.toBytes(c.getPort()));
    row.add(IntegerUtil.toBytes(c.getLocalPort()));
    row.add(StringUtil.encode(c.getUser(), charset));
    row.add(StringUtil.encode(c.getSchema(), charset));
    row.add(StringUtil.encode(c.getCharset() + ":" + c.getCharsetIndex(), charset));
    row.add(LongUtil.toBytes(c.getNetInBytes()));
    row.add(LongUtil.toBytes(c.getNetOutBytes()));
    row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
    ByteBuffer bb = c.getReadBuffer();
    row.add(IntegerUtil.toBytes(bb == null ? 0 : bb.capacity()));
    row.add(IntegerUtil.toBytes(c.getWriteQueue().size()));

    String txLevel = "";
    String txAutommit = "";
    if (c instanceof IServerConnection) {
      IServerConnection mysqlC = (IServerConnection) c;
      txLevel = mysqlC.getTxIsolation() + "";
      txAutommit = mysqlC.isAutocommit() + "";
    }
    row.add(txLevel.getBytes());
    row.add(txAutommit.getBytes());

    return row;
  }

}