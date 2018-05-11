/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.util.IntegerUtil;
import com.zhongan.dmds.commons.util.LongUtil;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IDmdsConfig;
import com.zhongan.dmds.core.IPhysicalDBNode;
import com.zhongan.dmds.core.IPhysicalDatasource;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 查看数据源信息
 */
public final class ShowDataSource {

  private static final int FIELD_COUNT = 10;
  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("DATANODE", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("W/R", Fields.FIELD_TYPE_VAR_STRING);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("ACTIVE", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("IDLE", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("SIZE", Fields.FIELD_TYPE_LONG);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("EXECUTE", Fields.FIELD_TYPE_LONGLONG);
    fields[i++].packetId = ++packetId;

    eof.packetId = ++packetId;
  }

  public static void execute(ManagerConnection c, String name) {
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
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    Map<String, List<IPhysicalDatasource>> dataSources = new HashMap<String, List<IPhysicalDatasource>>();
    if (null != name) {
      IPhysicalDBNode dn = conf.getDataNodes().get(name);
      if (dn != null) {
        List<IPhysicalDatasource> dslst = new LinkedList<IPhysicalDatasource>();
        dslst.addAll(dn.getDbPool().getAllDataSources());
        dataSources.put(dn.getName(), dslst);
      }

    } else {
      // add all

      for (IPhysicalDBNode dn : conf.getDataNodes().values()) {
        List<IPhysicalDatasource> dslst = new LinkedList<IPhysicalDatasource>();
        dslst.addAll(dn.getDbPool().getAllDataSources());
        dataSources.put(dn.getName(), dslst);
      }

    }

    for (Map.Entry<String, List<IPhysicalDatasource>> dsEntry : dataSources.entrySet()) {
      String dnName = dsEntry.getKey();
      for (IPhysicalDatasource ds : dsEntry.getValue()) {
        RowDataPacket row = getRow(dnName, ds, c.getCharset());
        row.packetId = ++packetId;
        buffer = row.write(buffer, c, true);
      }
    }

    // write last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // post write
    c.write(buffer);
  }

  private static RowDataPacket getRow(String dataNode, IPhysicalDatasource ds, String charset) {
    RowDataPacket row = new RowDataPacket(FIELD_COUNT);
    row.add(StringUtil.encode(dataNode, charset));
    row.add(StringUtil.encode(ds.getName(), charset));
    row.add(StringUtil.encode(ds.getConfig().getDbType(), charset));
    row.add(StringUtil.encode(ds.getConfig().getIp(), charset));
    row.add(IntegerUtil.toBytes(ds.getConfig().getPort()));
    row.add(StringUtil.encode(ds.isReadNode() ? "R" : "W", charset));
    row.add(IntegerUtil.toBytes(ds.getActiveCount()));
    row.add(IntegerUtil.toBytes(ds.getIdleCount()));
    row.add(IntegerUtil.toBytes(ds.getSize()));
    row.add(LongUtil.toBytes(ds.getExecuteCount()));
    return row;
  }

}