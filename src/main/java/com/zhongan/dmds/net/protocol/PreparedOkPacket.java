/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.protocol;

import com.zhongan.dmds.commons.mysql.BufferUtil;
import com.zhongan.dmds.core.NIOConnection;

import java.nio.ByteBuffer;

/**
 * <pre>
 * From server to client, in response to prepared statement initialization packet.
 * It is made up of:
 *   1.a PREPARE_OK packet
 *   2.if "number of parameters" > 0
 *       (field packets) as in a Result Set Header Packet
 *       (EOF packet)
 *   3.if "number of columns" > 0
 *       (field packets) as in a Result Set Header Packet
 *       (EOF packet)
 *
 * -----------------------------------------------------------------------------------------
 *
 *  Bytes              Name
 *  -----              ----
 *  1                  0 - marker for OK packet
 *  4                  statement_handler_id
 *  2                  number of columns in result set
 *  2                  number of parameters in query
 *  1                  filler (always 0)
 *  2                  warning count
 *
 *  &#64;see http://dev.mysql.com/doc/internals/en/prepared-statement-initialization-packet.html
 * </pre>
 */
public class PreparedOkPacket extends MySQLPacket {

  public byte flag;
  public long statementId;
  public int columnsNumber;
  public int parametersNumber;
  public byte filler;
  public int warningCount;

  public PreparedOkPacket() {
    this.flag = 0;
    this.filler = 0;
  }

  @Override
  public ByteBuffer write(ByteBuffer buffer, NIOConnection c, boolean writeSocketIfFull) {
    int size = calcPacketSize();
    buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize() + size, writeSocketIfFull);
    BufferUtil.writeUB3(buffer, size);
    buffer.put(packetId);
    buffer.put(flag);
    BufferUtil.writeUB4(buffer, statementId);
    BufferUtil.writeUB2(buffer, columnsNumber);
    BufferUtil.writeUB2(buffer, parametersNumber);
    buffer.put(filler);
    BufferUtil.writeUB2(buffer, warningCount);
    return buffer;
  }

  @Override
  public int calcPacketSize() {
    return 12;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL PreparedOk Packet";
  }

}