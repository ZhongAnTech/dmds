/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.statistic.DataSourceSyncRecorder;
import com.zhongan.dmds.commons.statistic.DataSourceSyncRecorder.Record;
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
import com.zhongan.dmds.sqlParser.ManagerParseShow;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ShowDatasourceSynDetail {

  private static final int FIELD_COUNT = 8;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();
  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("name", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("host", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("port", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("Master_Host", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("Master_Port", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("Master_Use", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("TIME", Fields.FIELD_TYPE_DATETIME);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("Seconds_Behind_Master", Fields.FIELD_TYPE_LONG);
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

    String name = ManagerParseShow.getWhereParameter(stmt);
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
    Map<String, IPhysicalDBPool> dataHosts = conf.getDataHosts();
    for (IPhysicalDBPool pool : dataHosts.values()) {
      for (IPhysicalDatasource ds : pool.getAllDataSources()) {
        IDBHeartbeat hb = ds.getHeartbeat();
        DataSourceSyncRecorder record = hb.getAsynRecorder();
        Map<String, String> states = record.getRecords();
        if (name.equals(ds.getName())) {
          List<Record> data = record.getAsynRecords();
          for (Record r : data) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);

            row.add(StringUtil.encode(ds.getName(), charset));
            row.add(StringUtil.encode(ds.getConfig().getIp(), charset));
            row.add(LongUtil.toBytes(ds.getConfig().getPort()));
            row.add(StringUtil.encode(states.get("Master_Host"), charset));
            row.add(LongUtil.toBytes(Long.valueOf(states.get("Master_Port"))));
            row.add(StringUtil.encode(states.get("Master_Use"), charset));
            String time = sdf.format(new Date(r.getTime()));
            row.add(StringUtil.encode(time, charset));
            row.add(LongUtil.toBytes((Long) r.getValue()));

            list.add(row);
          }
          break;
        }

      }
    }
    return list;
  }
}
