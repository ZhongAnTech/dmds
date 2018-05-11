/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio.handler;

import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.ResponseHandler;
import com.zhongan.dmds.net.mysql.nio.MySQLConnection;
import com.zhongan.dmds.net.protocol.CommandPacket;
import com.zhongan.dmds.net.protocol.ErrorPacket;
import com.zhongan.dmds.net.protocol.MySQLPacket;
import com.zhongan.dmds.net.session.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class KillConnectionHandler implements ResponseHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(KillConnectionHandler.class);

  private final MySQLConnection killee;
  private final NonBlockingSession session;

  public KillConnectionHandler(BackendConnection killee, NonBlockingSession session) {
    this.killee = (MySQLConnection) killee;
    this.session = session;
  }

  @Override
  public void connectionAcquired(BackendConnection conn) {
    MySQLConnection mysqlCon = (MySQLConnection) conn;
    conn.setResponseHandler(this);
    CommandPacket packet = new CommandPacket();
    packet.packetId = 0;
    packet.command = MySQLPacket.COM_QUERY;
    packet.arg = new StringBuilder("KILL ").append(killee.getThreadId()).toString().getBytes();
    packet.write(mysqlCon);
  }

  @Override
  public void connectionError(Throwable e, BackendConnection conn) {
    killee.close("exception:" + e.toString());
  }

  @Override
  public void okResponse(byte[] ok, BackendConnection conn) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("kill connection success connection id:" + killee.getThreadId());
    }
    conn.release();
    killee.close("killed");

  }

  @Override
  public void rowEofResponse(byte[] eof, BackendConnection conn) {
    LOGGER
        .warn(new StringBuilder().append("unexpected packet for ").append(conn).append(" bound by ")
            .append(session.getSource()).append(": field's eof").toString());
    conn.quit();
    killee.close("killed");
  }

  @Override
  public void errorResponse(byte[] data, BackendConnection conn) {
    ErrorPacket err = new ErrorPacket();
    err.read(data);
    String msg = null;
    try {
      msg = new String(err.message, conn.getCharset());
    } catch (UnsupportedEncodingException e) {
      msg = new String(err.message);
    }
    LOGGER.warn("kill backend connection " + killee + " failed: " + msg + " con:" + conn);
    conn.release();
    killee.close("exception:" + msg);
  }

  @Override
  public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof,
      BackendConnection conn) {
  }

  @Override
  public void rowResponse(byte[] row, BackendConnection conn) {
  }

  @Override
  public void writeQueueAvailable() {

  }

  @Override
  public void connectionClose(BackendConnection conn, String reason) {
  }

}