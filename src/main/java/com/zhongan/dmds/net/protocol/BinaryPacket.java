/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.protocol;

import com.zhongan.dmds.commons.mysql.BufferUtil;
import com.zhongan.dmds.commons.protocol.StreamUtil;
import com.zhongan.dmds.core.NIOConnection;
import com.zhongan.dmds.net.BackendNIOConnection;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class BinaryPacket extends MySQLPacket {

  public static final byte OK = 1;
  public static final byte ERROR = 2;
  public static final byte HEADER = 3;
  public static final byte FIELD = 4;
  public static final byte FIELD_EOF = 5;
  public static final byte ROW = 6;
  public static final byte PACKET_EOF = 7;

  public byte[] data;

  public void read(InputStream in) throws IOException {
    packetLength = StreamUtil.readUB3(in);
    packetId = StreamUtil.read(in);
    byte[] ab = new byte[packetLength];
    StreamUtil.read(in, ab, 0, ab.length);
    data = ab;
  }

  @Override
  public ByteBuffer write(ByteBuffer buffer, NIOConnection c, boolean writeSocketIfFull) {
    buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize(), writeSocketIfFull);
    BufferUtil.writeUB3(buffer, calcPacketSize());
    buffer.put(packetId);
    buffer = c.writeToBuffer(data, buffer);
    return buffer;
  }

  @Override
  public void write(BackendNIOConnection c) {
    ByteBuffer buffer = c.allocate();
    buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize() + calcPacketSize(), false);
    BufferUtil.writeUB3(buffer, calcPacketSize());
    buffer.put(packetId);
    buffer.put(data);
    c.write(buffer);
  }

  @Override
  public int calcPacketSize() {
    return data == null ? 0 : data.length;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL Binary Packet";
  }

}