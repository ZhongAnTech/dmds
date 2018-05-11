/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio.handler;

import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.ResponseHandler;
import com.zhongan.dmds.net.protocol.ErrorPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * heartbeat check for mysql connections
 */
public class ConnectionHeartBeatHandler implements ResponseHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionHeartBeatHandler.class);
  protected final ReentrantLock lock = new ReentrantLock();
  private final ConcurrentHashMap<Long, HeartBeatCon> allCons = new ConcurrentHashMap<Long, HeartBeatCon>();

  public void doHeartBeat(BackendConnection conn, String sql) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("do heartbeat for con " + conn);
    }

    try {
      HeartBeatCon hbCon = new HeartBeatCon(conn);
      boolean notExist = (allCons.putIfAbsent(hbCon.conn.getId(), hbCon) == null);
      if (notExist) {
        conn.setResponseHandler(this);
        conn.query(sql);
      }
    } catch (Exception e) {
      executeException(conn, e);
    }
  }

  /**
   * remove timeout connections
   */
  public void abandTimeOuttedConns() {
    if (allCons.isEmpty()) {
      return;
    }
    Collection<BackendConnection> abandCons = new LinkedList<BackendConnection>();
    long curTime = System.currentTimeMillis();
    Iterator<Entry<Long, HeartBeatCon>> itors = allCons.entrySet().iterator();
    while (itors.hasNext()) {
      HeartBeatCon hbCon = itors.next().getValue();
      if (hbCon.timeOutTimestamp < curTime) {
        abandCons.add(hbCon.conn);
        itors.remove();
      }
    }

    if (!abandCons.isEmpty()) {
      for (BackendConnection con : abandCons) {
        try {
          con.close("heartbeat timeout ");
        } catch (Exception e) {
          LOGGER.warn("close err:" + e);
        }
      }
    }

  }

  @Override
  public void connectionAcquired(BackendConnection conn) {
    // not called
  }

  @Override
  public void connectionError(Throwable e, BackendConnection conn) {
    // not called

  }

  @Override
  public void errorResponse(byte[] data, BackendConnection conn) {
    removeFinished(conn);
    ErrorPacket err = new ErrorPacket();
    err.read(data);
    LOGGER.warn("errorResponse " + err.errno + " " + new String(err.message));
    conn.release();

  }

  @Override
  public void okResponse(byte[] ok, BackendConnection conn) {
    boolean executeResponse = conn.syncAndExcute();
    if (executeResponse) {
      removeFinished(conn);
      conn.release();
    }

  }

  @Override
  public void rowResponse(byte[] row, BackendConnection conn) {
  }

  @Override
  public void rowEofResponse(byte[] eof, BackendConnection conn) {
    removeFinished(conn);
    conn.release();
  }

  private void executeException(BackendConnection c, Throwable e) {
    removeFinished(c);
    LOGGER.warn("executeException   ", e);
    c.close("heatbeat exception:" + e);

  }

  private void removeFinished(BackendConnection con) {
    Long id = ((BackendConnection) con).getId();
    this.allCons.remove(id);
  }

  @Override
  public void writeQueueAvailable() {

  }

  @Override
  public void connectionClose(BackendConnection conn, String reason) {
    removeFinished(conn);
    LOGGER.warn("connection closed " + conn + " reason:" + reason);
  }

  @Override
  public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof,
      BackendConnection conn) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("received field eof  from " + conn);
    }
  }

}

class HeartBeatCon {

  public final long timeOutTimestamp;
  public final BackendConnection conn;

  public HeartBeatCon(BackendConnection conn) {
    super();
    this.timeOutTimestamp = System.currentTimeMillis() + 20 * 1000L;
    this.conn = conn;
  }

}