/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.parse.Pair;
import com.zhongan.dmds.commons.statistic.HeartbeatRecorder;
import com.zhongan.dmds.commons.util.IntegerUtil;
import com.zhongan.dmds.commons.util.LongUtil;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.core.*;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;
import com.zhongan.dmds.sqlParser.ManagerParseHeartbeat;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ShowHeartbeatDetail {

  private static final int FIELD_COUNT = 6;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("TIME", Fields.FIELD_TYPE_DATETIME);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    eof.packetId = ++packetId;
  }

  public static void response(ManagerConnection c, String stmt) {
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
    Pair<String, String> pair = ManagerParseHeartbeat.getPair(stmt);
    String name = pair.getValue();
    for (RowDataPacket row : getRows(name, c.getCharset())) {
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

  private static List<RowDataPacket> getRows(String name, String charset) {
    List<RowDataPacket> list = new LinkedList<RowDataPacket>();
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    // host nodes
    String type = "";
    String ip = "";
    int port = 0;
    IDBHeartbeat hb = null;

    Map<String, IPhysicalDBPool> dataHosts = conf.getDataHosts();
    for (IPhysicalDBPool pool : dataHosts.values()) {
      for (IPhysicalDatasource ds : pool.getAllDataSources()) {
        if (name.equals(ds.getName())) {
          hb = ds.getHeartbeat();
          type = ds.getConfig().getDbType();
          ip = ds.getConfig().getIp();
          port = ds.getConfig().getPort();
          break;
        }
      }
    }
    if (hb != null) {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      List<HeartbeatRecorder.Record> heatbeartRecorders = hb.getRecorder().getRecordsAll();
      for (HeartbeatRecorder.Record record : heatbeartRecorders) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(name, charset));
        row.add(StringUtil.encode(type, charset));
        row.add(StringUtil.encode(ip, charset));
        row.add(IntegerUtil.toBytes(port));
        long time = record.getTime();
        String timeStr = sdf.format(new Date(time));
        row.add(StringUtil.encode(timeStr, charset));
        row.add(LongUtil.toBytes(record.getValue()));

        list.add(row);
      }
    } else {
      RowDataPacket row = new RowDataPacket(FIELD_COUNT);
      row.add(null);
      row.add(null);
      row.add(null);
      row.add(null);
      row.add(null);
      row.add(null);
      list.add(row);
    }

    return list;
  }

}