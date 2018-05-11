/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.stat.SqlFrequency;
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
import java.util.List;
import java.util.Map;

/**
 * 查询高频 SQL
 */
public final class ShowSQLHigh {

  private static final int FIELD_COUNT = 9;
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

    fields[i] = PacketUtil.getField("FREQUENCY", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("AVG_TIME", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;
    fields[i] = PacketUtil.getField("MAX_TIME", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;
    fields[i] = PacketUtil.getField("MIN_TIME", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;
    fields[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
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
    /*
     * List<Map.Entry<String, SqlFrequency>> list =
     * HighFrequencySqlAnalyzer.getInstance().getSqlFrequency( isClear ); if ( list
     * != null ) { for (int i = 0; i < list.size(); i++) { SqlFrequency sqlFrequency
     * = list.get(i).getValue(); RowDataPacket row = getRow(i,
     * sqlFrequency.getSql(), sqlFrequency.getCount(),
     * sqlFrequency.getAvgTime(),sqlFrequency.getMaxTime(),sqlFrequency.getMinTime()
     * ,sqlFrequency.getExecuteTime(),sqlFrequency.getLastTime(), c.getCharset());
     * row.packetId = ++packetId; buffer = row.write(buffer, c,true); } }
     */
    Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
    for (UserStat userStat : statMap.values()) {
      String user = userStat.getUser();
      List<Map.Entry<String, SqlFrequency>> list = userStat.getSqlHigh().getSqlFrequency(isClear);
      if (list != null) {
        for (int i = 0; i < list.size(); i++) {
          SqlFrequency sqlFrequency = list.get(i).getValue();
          RowDataPacket row = getRow(i, user, sqlFrequency.getSql(), sqlFrequency.getCount(),
              sqlFrequency.getAvgTime(), sqlFrequency.getMaxTime(), sqlFrequency.getMinTime(),
              sqlFrequency.getExecuteTime(), sqlFrequency.getLastTime(), c.getCharset());
          row.packetId = ++packetId;
          buffer = row.write(buffer, c, true);
        }
      }
    }
    // write last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // write buffer
    c.write(buffer);
  }

  private static RowDataPacket getRow(int i, String user, String sql, int count, long avgTime,
      long maxTime,
      long minTime, long executTime, long lastTime, String charset) {
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(LongUtil.toBytes(i));
    row.add(StringUtil.encode(user, charset));
    row.add(LongUtil.toBytes(count));
    row.add(LongUtil.toBytes(avgTime));
    row.add(LongUtil.toBytes(maxTime));
    row.add(LongUtil.toBytes(minTime));
    row.add(LongUtil.toBytes(executTime));
    row.add(LongUtil.toBytes(lastTime));
    row.add(StringUtil.encode(sql, charset));
    return row;
  }

}
