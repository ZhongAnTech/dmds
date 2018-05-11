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

public class ShowDatasourceCluster {

  private static final int FIELD_COUNT = 17;
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

    fields[i] = PacketUtil.getField("wsrep_incoming_addresses", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_cluster_size", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_cluster_status", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_connected", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_flow_control_paused", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_local_state_comment", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_ready", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_flow_control_paused_ns", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_flow_control_recv", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_local_bf_aborts", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_local_recv_queue_avg", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_local_send_queue_avg", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_apply_oool", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("wsrep_apply_oooe", Fields.FIELD_TYPE_VAR_STRING);
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

          row.add(StringUtil.encode(states.get("wsrep_incoming_addresses") == null ? ""
              : states.get("wsrep_incoming_addresses"), charset));
          row.add(StringUtil.encode(
              states.get("wsrep_cluster_size") == null ? "" : states.get("wsrep_cluster_size"),
              charset));
          row.add(StringUtil.encode(
              states.get("wsrep_cluster_status") == null ? "" : states.get("wsrep_cluster_status"),
              charset));
          row.add(StringUtil.encode(
              states.get("wsrep_connected") == null ? "" : states.get("wsrep_connected"), charset));
          row.add(StringUtil.encode(states.get("wsrep_flow_control_paused") == null ? ""
              : states.get("wsrep_flow_control_paused"), charset));
          row.add(StringUtil.encode(states.get("wsrep_local_state_comment") == null ? ""
              : states.get("wsrep_local_state_comment"), charset));
          row.add(
              StringUtil.encode(states.get("wsrep_ready") == null ? "" : states.get("wsrep_ready"),
                  charset));
          row.add(StringUtil.encode(states.get("wsrep_flow_control_paused_ns") == null ? ""
              : states.get("wsrep_flow_control_paused_ns"), charset));
          row.add(StringUtil.encode(
              states.get("wsrep_flow_control_recv") == null ? ""
                  : states.get("wsrep_flow_control_recv"),
              charset));
          row.add(StringUtil.encode(
              states.get("wsrep_local_bf_aborts") == null ? ""
                  : states.get("wsrep_local_bf_aborts"),
              charset));
          row.add(StringUtil.encode(states.get("wsrep_local_recv_queue_avg") == null ? ""
              : states.get("wsrep_local_recv_queue_avg"), charset));
          row.add(StringUtil.encode(states.get("wsrep_local_send_queue_avg") == null ? ""
              : states.get("wsrep_local_recv_queue_avg"), charset));
          row.add(StringUtil.encode(
              states.get("wsrep_apply_oool") == null ? "" : states.get("wsrep_apply_oool"),
              charset));
          row.add(StringUtil.encode(
              states.get("wsrep_apply_oooe") == null ? "" : states.get("wsrep_apply_oooe"),
              charset));

          list.add(row);
        }
      }
    }
    return list;
  }

}