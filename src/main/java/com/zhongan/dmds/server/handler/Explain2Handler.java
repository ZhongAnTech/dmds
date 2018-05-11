/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.handler;

import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.mysql.nio.handler.SingleNodeHandler;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;
import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class Explain2Handler {

  private static final Logger logger = LoggerFactory.getLogger(Explain2Handler.class);
  private static final RouteResultsetNode[] EMPTY_ARRAY = new RouteResultsetNode[1];
  private static final int FIELD_COUNT = 2;
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];

  static {
    fields[0] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
    fields[1] = PacketUtil.getField("MSG", Fields.FIELD_TYPE_VAR_STRING);
  }

  public static void handle(String stmt, IServerConnection c, int offset) {

    try {
      stmt = stmt.substring(offset);
      if (!stmt.toLowerCase().contains("datanode=") || !stmt.toLowerCase().contains("sql=")) {
        showerror(stmt, c, "explain2 datanode=? sql=?");
        return;
      }
      String dataNode = stmt.substring(stmt.indexOf("=") + 1, stmt.indexOf("sql=")).trim();
      String sql = "explain " + stmt.substring(stmt.indexOf("sql=") + 4, stmt.length()).trim();

      if (dataNode == null || dataNode.isEmpty() || sql == null || sql.isEmpty()) {
        showerror(stmt, c, "dataNode or sql is null or empty");
        return;
      }

      RouteResultsetNode node = new RouteResultsetNode(dataNode, ServerParse.SELECT, sql);
      RouteResultset rrs = new RouteResultset(sql, ServerParse.SELECT);
      EMPTY_ARRAY[0] = node;
      rrs.setNodes(EMPTY_ARRAY);
      SingleNodeHandler singleNodeHandler = new SingleNodeHandler(rrs, c.getSession2());
      singleNodeHandler.execute();
    } catch (Exception e) {
      logger.error(e.getMessage(), e.getCause());
      e.printStackTrace();
      showerror(stmt, c, e.getMessage());
    }
  }

  private static void showerror(String stmt, IServerConnection c, String msg) {
    ByteBuffer buffer = c.allocate();
    // write header
    ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    byte packetId = header.packetId;
    buffer = header.write(buffer, c, true);

    // write fields
    for (FieldPacket field : fields) {
      field.packetId = ++packetId;
      buffer = field.write(buffer, c, true);
    }

    // write eof
    EOFPacket eof = new EOFPacket();
    eof.packetId = ++packetId;
    buffer = eof.write(buffer, c, true);

    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(StringUtil.encode(stmt, c.getCharset()));
    row.add(StringUtil.encode(msg, c.getCharset()));
    row.packetId = ++packetId;
    buffer = row.write(buffer, c, true);

    // write last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // post write
    c.write(buffer);
  }
}