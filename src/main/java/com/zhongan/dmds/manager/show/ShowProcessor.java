/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.buffer.BufferPool;
import com.zhongan.dmds.commons.util.IntegerUtil;
import com.zhongan.dmds.commons.util.LongUtil;
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

/**
 * 查看处理器状态
 */
public final class ShowProcessor {

  private static final int FIELD_COUNT = 12;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("REACT_COUNT", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("R_QUEUE", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("W_QUEUE", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("FREE_BUFFER", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("TOTAL_BUFFER", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("BU_PERCENT", Fields.FIELD_TYPE_TINY);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("BU_WARNS", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("FC_COUNT", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("BC_COUNT", Fields.FIELD_TYPE_LONG);
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

  private static RowDataPacket getRow(INIOProcessor processor, String charset) {
    BufferPool bufferPool = processor.getBufferPool();
    long bufferSize = bufferPool.size();
    long bufferCapacity = bufferPool.capacity();
    long newCreated = bufferPool.getNewCreated();
    long bufferUsagePercent = (bufferCapacity - bufferSize) * 100 / bufferCapacity;
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(processor.getName().getBytes());
    row.add(LongUtil.toBytes(processor.getNetInBytes()));
    row.add(LongUtil.toBytes(processor.getNetOutBytes()));
    row.add(LongUtil.toBytes(0));
    row.add(IntegerUtil.toBytes(0));
    row.add(IntegerUtil.toBytes(processor.getWriteQueueSize()));
    row.add(LongUtil.toBytes(bufferSize));
    row.add(LongUtil.toBytes(bufferCapacity));
    row.add(LongUtil.toBytes(bufferUsagePercent));
    row.add(LongUtil.toBytes(newCreated));
    row.add(IntegerUtil.toBytes(processor.getFrontends().size()));
    row.add(IntegerUtil.toBytes(processor.getBackends().size()));
    return row;
  }

}