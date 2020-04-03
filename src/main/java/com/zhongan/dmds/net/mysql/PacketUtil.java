/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql;

import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.config.util.CharsetUtil;
import com.zhongan.dmds.net.protocol.BinaryPacket;
import com.zhongan.dmds.net.protocol.ErrorPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;

import java.io.UnsupportedEncodingException;

public class PacketUtil {

  private static final String CODE_PAGE_1252 = "Cp1252";

  public static final ResultSetHeaderPacket getHeader(int fieldCount) {
    ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
    packet.packetId = 1;
    packet.fieldCount = fieldCount;
    return packet;
  }

  public static byte[] encode(String src, String charset) {
    if (src == null) {
      return null;
    }
    try {
      return src.getBytes(charset);
    } catch (UnsupportedEncodingException e) {
      return src.getBytes();
    }
  }

  public static final FieldPacket getField(String name, String orgName, int type) {
    FieldPacket packet = new FieldPacket();
    packet.charsetIndex = CharsetUtil.getIndex(CODE_PAGE_1252);
    packet.name = encode(name, CODE_PAGE_1252);
    packet.orgName = encode(orgName, CODE_PAGE_1252);
    packet.type = (byte) type;
    return packet;
  }

  public static final FieldPacket getField(String name, int type) {
    FieldPacket packet = new FieldPacket();
    packet.charsetIndex = CharsetUtil.getIndex(CODE_PAGE_1252);
    packet.name = encode(name, CODE_PAGE_1252);
    packet.type = (byte) type;
    return packet;
  }

  public static final ErrorPacket getShutdown() {
    ErrorPacket error = new ErrorPacket();
    error.packetId = 1;
    error.errno = ErrorCode.ER_SERVER_SHUTDOWN;
    error.message = "The server has been shutdown".getBytes();
    return error;
  }

  public static final FieldPacket getField(BinaryPacket src, String fieldName) {
    FieldPacket field = new FieldPacket();
    field.read(src);
    field.name = encode(fieldName, CODE_PAGE_1252);
    field.packetLength = field.calcPacketSize();
    return field;
  }

}