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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 打印DmdsServer所支持的语句
 */
public final class ShowHelp {

  private static final int FIELD_COUNT = 2;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("STATEMENT", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("DESCRIPTION", Fields.FIELD_TYPE_VAR_STRING);
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
    for (String key : keys) {
      RowDataPacket row = getRow(key, helps.get(key), c.getCharset());
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

  private static RowDataPacket getRow(String stmt, String desc, String charset) {
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(StringUtil.encode(stmt, charset));
    row.add(StringUtil.encode(desc, charset));
    return row;
  }

  private static final Map<String, String> helps = new LinkedHashMap<String, String>();
  private static final List<String> keys = new LinkedList<String>();

  static {
    // show
    helps.put("show @@time.current", "Report current timestamp");
    helps.put("show @@time.startup", "Report startup timestamp");
    helps.put("show @@version", "Report DMDS Server version");
    helps.put("show @@server", "Report server status");
    helps.put("show @@threadpool", "Report threadPool status");
    helps.put("show @@database", "Report databases");
    helps.put("show @@datanode", "Report dataNodes");
    helps.put("show @@datanode where schema = ?", "Report dataNodes");
    helps.put("show @@datasource", "Report dataSources");
    helps.put("show @@datasource where dataNode = ?", "Report dataSources");
    helps.put("show @@datasource.synstatus", "Report datasource data synchronous");
    helps.put("show @@datasource.syndetail where name=?",
        "Report datasource data synchronous detail");
    helps.put("show @@datasource.cluster", "Report datasource galary cluster variables");
    helps.put("show @@processor", "Report processor status");
    helps.put("show @@command", "Report commands status");
    helps.put("show @@connection", "Report connection status");
    helps.put("show @@cache", "Report system cache usage");
    helps.put("show @@backend", "Report backend connection status");
    helps.put("show @@session", "Report front session details");
    helps.put("show @@connection.sql", "Report connection sql");
    helps.put("show @@sql.execute", "Report execute status");
    helps.put("show @@sql.detail where id = ?", "Report execute detail status");
    helps.put("show @@sql", "Report SQL list");
    // helps.put("show @@sql where id = ?", "Report specify SQL");
    helps.put("show @@sql.high", "Report Hight Frequency SQL");
    helps.put("show @@sql.slow", "Report slow SQL");
    helps.put("show @@sql.sum", "Report  User RW Stat ");
    helps.put("show @@sql.sum.user", "Report  User RW Stat ");
    helps.put("show @@sql.sum.table", "Report  Table RW Stat ");
    helps.put("show @@parser", "Report parser status");
    helps.put("show @@router", "Report router status");
    helps.put("show @@heartbeat", "Report heartbeat status");
    helps.put("show @@heartbeat.detail where name=?", "Report heartbeat current detail");
    helps.put("show @@slow where schema = ?", "Report schema slow sql");
    helps.put("show @@slow where datanode = ?", "Report datanode slow sql");
    helps.put("show @@sysparam", "Report system param");
    helps.put("show @@syslog limit=?", "Report system dmds.log");
    helps.put("show @@white", "show dmds white host ");
    helps.put("show @@white.set=?,?", "set dmds white host,[ip,user]");

    // switch
    helps.put("switch @@datasource name:index", "Switch dataSource");

    // kill
    helps.put("kill @@connection id1,id2,...", "Kill the specified connections");

    // stop
    helps.put("stop @@heartbeat name:time", "Pause dataNode heartbeat");

    // reload
    helps.put("reload @@config", "Reload basic config from file");
    helps.put("reload @@config_all", "Reload all config from file");
    helps.put("reload @@route", "Reload route config from file");
    helps.put("reload @@user", "Reload user config from file");
    helps.put("reload @@sqlslow=", "Set Slow SQL Time(ms)");
    helps.put("reload @@user_stat", "Reset show @@sql  @@sql.sum @@sql.slow");
    // rollback
    helps.put("rollback @@config", "Rollback all config from memory");
    helps.put("rollback @@route", "Rollback route config from memory");
    helps.put("rollback @@user", "Rollback user config from memory");

    // open/close sql stat
    helps.put("reload @@sqlstat=open", "Open real-time sql stat analyzer");
    helps.put("reload @@sqlstat=close", "Close real-time sql stat analyzer");

    // offline/online
    helps.put("offline", "Change DMDS status to OFF");
    helps.put("online", "Change DMDS status to ON");

    // clear
    helps.put("clear @@slow where schema = ?", "Clear slow sql by schema");
    helps.put("clear @@slow where datanode = ?", "Clear slow sql by datanode");

    // list sort
    keys.addAll(helps.keySet());
  }

}