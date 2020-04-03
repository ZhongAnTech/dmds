/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public final class ShowVariables {

  private static final int FIELD_COUNT = 2;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("VARIABLE_NAME", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("VALUE", Fields.FIELD_TYPE_VAR_STRING);
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
    for (Map.Entry<String, String> e : variables.entrySet()) {
      RowDataPacket row = getRow(e.getKey(), e.getValue(), c.getCharset());
      row.packetId = ++packetId;
      buffer = row.write(buffer, c, true);
    }

    // write lastEof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // write buffer
    c.write(buffer);
  }

  private static RowDataPacket getRow(String name, String value, String charset) {
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(StringUtil.encode(name, charset));
    row.add(StringUtil.encode(value, charset));
    return row;
  }

  private static final Map<String, String> variables = new HashMap<String, String>();

  static {
    variables.put("character_set_client", "utf8");
    variables.put("character_set_connection", "utf8");
    variables.put("character_set_results", "utf8");
    variables.put("character_set_server", "utf8");
    variables.put("init_connect", "");
    variables.put("interactive_timeout", "172800");
    variables.put("lower_case_table_names", "1");
    variables.put("max_allowed_packet", "16777216");
    variables.put("net_buffer_length", "8192");
    variables.put("net_write_timeout", "60");
    variables.put("query_cache_size", "0");
    variables.put("query_cache_type", "OFF");
    variables.put("sql_mode", "STRICT_TRANS_TABLES");
    variables.put("system_time_zone", "CST");
    variables.put("time_zone", "SYSTEM");
    variables.put("lower_case_table_names", "1");
    variables.put("tx_isolation", "REPEATABLE-READ");
    variables.put("wait_timeout", "172800");
  }

}