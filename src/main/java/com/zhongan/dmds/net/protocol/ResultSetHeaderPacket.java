/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.protocol;

import com.zhongan.dmds.commons.mysql.BufferUtil;
import com.zhongan.dmds.commons.mysql.MySQLMessage;
import com.zhongan.dmds.core.NIOConnection;

import java.nio.ByteBuffer;

/**
 * From server to client after command, if no error and result set -- that is, if the command was a
 * query which returned a result set. The Result Set Header Packet is the first of several, possibly
 * many, packets that the server sends for result sets. The order of packets for a result set is:
 *
 * <pre>
 * (Result Set Header Packet)   the number of columns
 * (Field Packets)              column descriptors
 * (EOF Packet)                 marker: end of Field Packets
 * (Row Data Packets)           row contents
 * (EOF Packet)                 marker: end of Data Packets
 *
 * Bytes                        Name
 * -----                        ----
 * 1-9   (Length-Coded-Binary)  field_count
 * 1-9   (Length-Coded-Binary)  extra
 *
 * &#64;see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Result_Set_Header_Packet
 * </pre>
 */
public class ResultSetHeaderPacket extends MySQLPacket {

  public int fieldCount;
  public long extra;

  public void read(byte[] data) {
    MySQLMessage mm = new MySQLMessage(data);
    this.packetLength = mm.readUB3();
    this.packetId = mm.read();
    this.fieldCount = (int) mm.readLength();
    if (mm.hasRemaining()) {
      this.extra = mm.readLength();
    }
  }

  @Override
  public ByteBuffer write(ByteBuffer buffer, NIOConnection c, boolean writeSocketIfFull) {
    int size = calcPacketSize();
    buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize() + size, writeSocketIfFull);
    BufferUtil.writeUB3(buffer, size);
    buffer.put(packetId);
    BufferUtil.writeLength(buffer, fieldCount);
    if (extra > 0) {
      BufferUtil.writeLength(buffer, extra);
    }
    return buffer;
  }

  @Override
  public int calcPacketSize() {
    int size = BufferUtil.getLength(fieldCount);
    if (extra > 0) {
      size += BufferUtil.getLength(extra);
    }
    return size;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL ResultSetHeader Packet";
  }

}