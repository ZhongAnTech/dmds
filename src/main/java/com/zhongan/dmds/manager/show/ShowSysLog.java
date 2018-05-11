/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.Fields;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.mysql.PacketUtil;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.FieldPacket;
import com.zhongan.dmds.net.protocol.ResultSetHeaderPacket;
import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Show @@SYSLOG LIMIT=50
 */
public class ShowSysLog {

  private static final int FIELD_COUNT = 2;

  private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof = new EOFPacket();

  static {
    int i = 0;
    byte packetId = 0;
    header.packetId = ++packetId;

    fields[i] = PacketUtil.getField("DATE", Fields.FIELD_TYPE_VARCHAR);
    fields[i++].packetId = ++packetId;

    fields[i] = PacketUtil.getField("LOG", Fields.FIELD_TYPE_VARCHAR);
    fields[i++].packetId = ++packetId;

    eof.packetId = ++packetId;
  }

  public static void execute(ManagerConnection c, int numLines) {
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

    String filename =
        SystemConfig.getHomePath() + File.separator + "logs" + File.separator + "dmds.log";

    String[] lines = getLinesByLogFile(filename, numLines);

    boolean linesIsEmpty = true;
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      if (line != null) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(line.substring(0, 19), c.getCharset()));
        row.add(StringUtil.encode(line.substring(19, line.length()), c.getCharset()));
        row.packetId = ++packetId;
        buffer = row.write(buffer, c, true);

        linesIsEmpty = false;
      }
    }

    if (linesIsEmpty) {
      RowDataPacket row = new RowDataPacket(FIELD_COUNT);
      row.add(StringUtil.encode("NULL", c.getCharset()));
      row.add(StringUtil.encode("NULL", c.getCharset()));
      row.packetId = ++packetId;
      buffer = row.write(buffer, c, true);
    }

    // write last eof
    EOFPacket lastEof = new EOFPacket();
    lastEof.packetId = ++packetId;
    buffer = lastEof.write(buffer, c, true);

    // write buffer
    c.write(buffer);
  }

  private static String[] getLinesByLogFile(String filename, int numLines) {
    String lines[] = new String[numLines];

    BufferedReader in = null;
    try {
      //获取长度
      int start = 0;
      int totalNumLines = 0;

      File logFile = new File(filename);
      in = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "UTF-8"));

      String line;
      while ((line = in.readLine()) != null) {
        totalNumLines++;
      }
      in.close();

      in = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "UTF-8"));

      // 跳过行
      start = totalNumLines - numLines;
      if (start < 0) {
        start = 0;
      }
      for (int i = 0; i < start; i++) {
        in.readLine();
      }

      // DESC
      int i = 0;
      int end = lines.length - 1;
      while ((line = in.readLine()) != null && i < numLines) {
        lines[end - i] = line;
        i++;
      }
      numLines = start + i;

    } catch (FileNotFoundException ex) {
    } catch (UnsupportedEncodingException e) {
    } catch (IOException e) {
    } finally {
      if (in != null) {
        try {
          in.close();
          in = null;
        } catch (IOException e) {
        }
      }
    }
    return lines;
  }
}
