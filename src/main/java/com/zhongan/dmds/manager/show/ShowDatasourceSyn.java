/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.statistic.DataSourceSyncRecorder;
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

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ShowDatasourceSyn {

  private static final int FIELD_COUNT = 12;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

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

    fields[i] = PacketUtil.getField("Seconds_Behind_Master", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("Slave_IO_Running", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("Slave_SQL_Running", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("Slave_IO_State", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("Connect_Retry", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("Last_IO_Error", Fields.FIELD_TYPE_VAR_STRING);
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

    for (RowDataPacket row : getRows(c.getCharset())) {
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

  private static List<RowDataPacket> getRows(String charset) {
    List<RowDataPacket> list = new LinkedList<RowDataPacket>();
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    // host nodes
    Map<String, IPhysicalDBPool> dataHosts = conf.getDataHosts();
    for (IPhysicalDBPool pool : dataHosts.values()) {
      for (IPhysicalDatasource ds : pool.getAllDataSources()) {
        IDBHeartbeat hb = ds.getHeartbeat();
        DataSourceSyncRecorder record = hb.getAsynRecorder();
        Map<String, String> states = record.getRecords();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        if (!states.isEmpty()) {
          row.add(StringUtil.encode(ds.getName(), charset));
          row.add(StringUtil.encode(ds.getConfig().getIp(), charset));
          row.add(LongUtil.toBytes(ds.getConfig().getPort()));
          row.add(StringUtil.encode(states.get("Master_Host"), charset));
          row.add(LongUtil.toBytes(Long.valueOf(states.get("Master_Port"))));
          row.add(StringUtil.encode(states.get("Master_Use"), charset));
          String secords = states.get("Seconds_Behind_Master");
          row.add(secords == null ? null : LongUtil.toBytes(Long.valueOf(secords)));
          row.add(StringUtil.encode(states.get("Slave_IO_Running"), charset));
          row.add(StringUtil.encode(states.get("Slave_SQL_Running"), charset));
          row.add(StringUtil.encode(states.get("Slave_IO_State"), charset));
          row.add(LongUtil.toBytes(Long.valueOf(states.get("Connect_Retry"))));
          row.add(StringUtil.encode(states.get("Last_IO_Error"), charset));

          list.add(row);
        }
      }
    }
    return list;
  }

}