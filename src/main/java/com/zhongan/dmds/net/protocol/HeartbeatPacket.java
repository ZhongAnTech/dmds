/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.protocol;

import com.zhongan.dmds.commons.mysql.BufferUtil;
import com.zhongan.dmds.commons.mysql.MySQLMessage;
import com.zhongan.dmds.net.BackendNIOConnection;

import java.nio.ByteBuffer;

/**
 * From client to server when the client do heartbeat between dmds cluster.
 *
 * <pre>
 * Bytes Name ----- ---- 1 command n id
 */
public class HeartbeatPacket extends MySQLPacket {

  public byte command;
  public long id;

  public void read(byte[] data) {
    MySQLMessage mm = new MySQLMessage(data);
    packetLength = mm.readUB3();
    packetId = mm.read();
    command = mm.read();
    id = mm.readLength();
  }

  @Override
  public void write(BackendNIOConnection c) {
    ByteBuffer buffer = c.allocate();
    BufferUtil.writeUB3(buffer, calcPacketSize());
    buffer.put(packetId);
    buffer.put(command);
    BufferUtil.writeLength(buffer, id);
    c.write(buffer);
  }

  @Override
  public int calcPacketSize() {
    return 1 + BufferUtil.getLength(id);
  }

  @Override
  protected String getPacketInfo() {
    return "Dmds Heartbeat Packet";
  }

}