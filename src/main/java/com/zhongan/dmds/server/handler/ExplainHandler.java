/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.handler;

import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;
import com.zhongan.dmds.net.util.SchemaUtil;
import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 2017.04 remove support for mycat seq (config: table autoincrement)
 */
public class ExplainHandler {

  private static final Logger logger = LoggerFactory.getLogger(ExplainHandler.class);
  private static final RouteResultsetNode[] EMPTY_ARRAY = new RouteResultsetNode[0];
  private static final int FIELD_COUNT = 2;
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];

  static {
    fields[0] = PacketUtil.getField("DATA_NODE", Fields.FIELD_TYPE_VAR_STRING);
    fields[1] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
  }

  public static void handle(String stmt, IServerConnection c, int offset) {
    stmt = stmt.substring(offset);

    RouteResultset rrs = getRouteResultset(c, stmt);
    if (rrs == null) {
      return;
    }

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

    // write rows
    RouteResultsetNode[] rrsn = (rrs != null) ? rrs.getNodes() : EMPTY_ARRAY;
    for (RouteResultsetNode node : rrsn) {
      RowDataPacket row = getRow(node, c.getCharset());
      row.packetId = ++packetId;
      buffer = row.write(buffer, c, true);
    }

    // write last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // post write
    c.write(buffer);

  }

  private static RowDataPacket getRow(RouteResultsetNode node, String charset) {
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(StringUtil.encode(node.getName(), charset));
    row.add(StringUtil.encode(node.getStatement().replaceAll("[\\t\\n\\r]", " "), charset));
    return row;
  }

  private static RouteResultset getRouteResultset(IServerConnection c, String stmt) {
    String db = c.getSchema();
    int sqlType = ServerParse.parse(stmt) & 0xff;
    if (db == null) {
      db = SchemaUtil.detectDefaultDb(stmt, sqlType);
      if (db == null) {
        c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
        return null;
      }
    }
    SchemaConfig schema = DmdsContext.getInstance().getConfig().getSchemas().get(db);
    if (schema == null) {
      c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
      return null;
    }
    try {
      // if(ServerParse.INSERT==sqlType /*&& isDmdsSeq(stmt, schema)*/) {
      // c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "insert sql using dmds seq,you
      // must provide primaryKey value for explain");
      // return null;
      // }
      SystemConfig system = DmdsContext.getInstance().getSystem();
      return DmdsContext.getInstance().getRouterservice()
          .route(system, schema, sqlType, stmt, c.getCharset(), c);
    } catch (Exception e) {
      StringBuilder s = new StringBuilder();
      logger.warn(s.append(c).append(stmt).toString() + " error:" + e);
      String msg = e.getMessage();
      c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
      return null;
    }
  }

  // private static boolean isDmdsSeq(String stmt, SchemaConfig schema) {
  // if(pattern.matcher(stmt).find()) {
  // return true;
  // }
  // SQLStatementParser parser =new MySqlStatementParser(stmt);
  // MySqlInsertStatement statement = (MySqlInsertStatement)
  // parser.parseStatement();
  // String tableName= statement.getTableName().getSimpleName();
  // TableConfig tableConfig= schema.getTables().get(tableName.toUpperCase());
  // if(tableConfig==null) {
  // return false;
  // }
  // if(tableConfig.isAutoIncrement()) {
  // boolean isHasIdInSql=false;
  // String primaryKey = tableConfig.getPrimaryKey();
  // List<SQLExpr> columns = statement.getColumns();
  // for (SQLExpr column : columns) {
  // String columnName = column.toString();
  // if(primaryKey.equalsIgnoreCase(columnName)) {
  // isHasIdInSql = true;
  // break;
  // }
  // }
  // if(!isHasIdInSql) {
  // return true;
  // }
  // }
  // return false;
  // }

}