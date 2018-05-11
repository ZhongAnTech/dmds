/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.stat.UserSqlStat;
import com.zhongan.dmds.commons.util.LongUtil;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;
import com.zhongan.dmds.net.stat.UserStat;
import com.zhongan.dmds.net.stat.UserStatAnalyzer;

import java.nio.ByteBuffer;
import java.util.Map;


/**
 * 查询用户最近执行的SQL记录
 */
public final class ShowSQL {

  private static final int FIELD_COUNT = 6;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VARCHAR);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("START_TIME", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("IP", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    eof.packetId = ++packetId;
  }

  public static void execute(ManagerConnection c, boolean isClear) {
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
    Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
    for (UserStat userStat : statMap.values()) {
      String user = userStat.getUser();
      UserSqlStat.Sql[] sqls = userStat.getSqlStat().getSqls();
      for (int i = sqls.length - 1; i >= 0; i--) {
        if (sqls[i] != null) {
          RowDataPacket row = getRow(user, sqls[i], i, c.getCharset());
          row.packetId = ++packetId;
          buffer = row.write(buffer, c, true);
        }
      }

      //读取SQL监控后清理
      if (isClear) {
        userStat.getSqlStat().clear();
      }
    }

    // write last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // write buffer
    c.write(buffer);
  }

  private static RowDataPacket getRow(String user, UserSqlStat.Sql sql, int idx, String charset) {

    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(LongUtil.toBytes(idx));
    row.add(StringUtil.encode(user, charset));
    row.add(LongUtil.toBytes(sql.getStartTime()));
    row.add(LongUtil.toBytes(sql.getExecuteTime()));
    row.add(StringUtil.encode(sql.getSql(), charset));
    row.add(StringUtil.encode(sql.getIp(), charset));
    return row;
  }

}