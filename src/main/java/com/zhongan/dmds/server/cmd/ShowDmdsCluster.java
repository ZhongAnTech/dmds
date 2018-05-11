/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.cmd;

import com.zhongan.dmds.commons.util.IntegerUtil;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.Alarms;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.config.model.DmdsCluster;
import com.zhongan.dmds.config.model.DmdsNode;
import com.zhongan.dmds.config.model.DmdsNodeConfig;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IDmdsConfig;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Base on ShowMyCATCluster
 */
public class ShowDmdsCluster {

  private static final Logger alarm = LoggerFactory.getLogger(ShowDmdsCluster.class);

  private static final int FIELD_COUNT = 2;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;
    fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;
    fields[i] = PacketUtil.getField("WEIGHT", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;
    eof.packetId = ++packetId;
  }

  public static void response(IServerConnection c) {
    ByteBuffer buffer = c.allocate();

    // write header
    buffer = header.write(buffer, c, true);

    // write field
    for (FieldPacket field : fields) {
      buffer = field.write(buffer, c, true);
    }

    // write eof
    buffer = eof.write(buffer, c, true);

    // write rows
    byte packetId = eof.packetId;
    for (RowDataPacket row : getRows(c)) {
      row.packetId = ++packetId;
      buffer = row.write(buffer, c, true);
    }

    // last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // post write
    c.write(buffer);
  }

  private static List<RowDataPacket> getRows(IServerConnection c) {
    List<RowDataPacket> rows = new LinkedList<RowDataPacket>();
    IDmdsConfig config = DmdsContext.getInstance().getConfig();
    DmdsCluster cluster = config.getCluster();
    Map<String, SchemaConfig> schemas = config.getSchemas();
    SchemaConfig schema = (c.getSchema() == null) ? null : schemas.get(c.getSchema());

    // 如果没有指定schema或者schema为null，则使用全部集群。
    if (schema == null) {
      Map<String, DmdsNode> nodes = cluster.getNodes();
      for (DmdsNode n : nodes.values()) {
        if (n != null && n.isOnline()) {
          rows.add(getRow(n, c.getCharset()));
        }
      }
    } else {

      Map<String, DmdsNode> nodes = cluster.getNodes();
      for (DmdsNode n : nodes.values()) {
        if (n != null && n.isOnline()) {
          rows.add(getRow(n, c.getCharset()));
        }
      }
    }

    if (rows.size() == 0) {
      alarm.error(Alarms.CLUSTER_EMPTY + c.toString());
    }

    return rows;
  }

  private static RowDataPacket getRow(DmdsNode node, String charset) {
    DmdsNodeConfig conf = node.getConfig();
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(StringUtil.encode(conf.getHost(), charset));
    row.add(IntegerUtil.toBytes(conf.getWeight()));
    return row;
  }

}