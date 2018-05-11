/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.cmd;

import com.zhongan.dmds.net.frontend.FrontendConnection;
import com.zhongan.dmds.net.mysql.PreparedStatement;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.PreparedOkPacket;

import java.nio.ByteBuffer;

public class PreparedStmtResponse {

  public static void response(PreparedStatement pstmt, FrontendConnection c) {
    byte packetId = 0;

    // write preparedOk packet
    PreparedOkPacket preparedOk = new PreparedOkPacket();
    preparedOk.packetId = ++packetId;
    preparedOk.statementId = pstmt.getId();
    preparedOk.columnsNumber = pstmt.getColumnsNumber();
    preparedOk.parametersNumber = pstmt.getParametersNumber();
    ByteBuffer buffer = preparedOk.write(c.allocate(), c, true);

    // write parameter field packet
    int parametersNumber = preparedOk.parametersNumber;
    if (parametersNumber > 0) {
      for (int i = 0; i < parametersNumber; i++) {
        FieldPacket field = new FieldPacket();
        field.packetId = ++packetId;
        buffer = field.write(buffer, c, true);
      }
      EOFPacket eof = new EOFPacket();
      eof.packetId = ++packetId;
      buffer = eof.write(buffer, c, true);
    }

    // write column field packet
    int columnsNumber = preparedOk.columnsNumber;
    if (columnsNumber > 0) {
      for (int i = 0; i < columnsNumber; i++) {
        FieldPacket field = new FieldPacket();
        field.packetId = ++packetId;
        buffer = field.write(buffer, c, true);
      }
      EOFPacket eof = new EOFPacket();
      eof.packetId = ++packetId;
      buffer = eof.write(buffer, c, true);
    }

    // send buffer
    c.write(buffer);
  }

}