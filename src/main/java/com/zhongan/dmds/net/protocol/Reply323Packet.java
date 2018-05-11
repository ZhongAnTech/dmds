/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.protocol;

import com.zhongan.dmds.commons.mysql.BufferUtil;
import com.zhongan.dmds.commons.protocol.StreamUtil;
import com.zhongan.dmds.net.BackendNIOConnection;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class Reply323Packet extends MySQLPacket {

  public byte[] seed;

  public void write(OutputStream out) throws IOException {
    StreamUtil.writeUB3(out, calcPacketSize());
    StreamUtil.write(out, packetId);
    if (seed == null) {
      StreamUtil.write(out, (byte) 0);
    } else {
      StreamUtil.writeWithNull(out, seed);
    }
  }

  @Override
  public void write(BackendNIOConnection c) {
    ByteBuffer buffer = c.allocate();
    BufferUtil.writeUB3(buffer, calcPacketSize());
    buffer.put(packetId);
    if (seed == null) {
      buffer.put((byte) 0);
    } else {
      BufferUtil.writeWithNull(buffer, seed);
    }
    c.write(buffer);
  }

  @Override
  public int calcPacketSize() {
    return seed == null ? 1 : seed.length + 1;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL Auth323 Packet";
  }

}