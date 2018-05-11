/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio.handler;

import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class GetConnectionHandler implements ResponseHandler {

  private final CopyOnWriteArrayList<BackendConnection> successCons;
  private static final Logger logger = LoggerFactory.getLogger(GetConnectionHandler.class);
  private final AtomicInteger finishedCount = new AtomicInteger(0);
  private final int total;

  public GetConnectionHandler(CopyOnWriteArrayList<BackendConnection> connsToStore,
      int totalNumber) {
    super();
    this.successCons = connsToStore;
    this.total = totalNumber;
  }

  public String getStatusInfo() {
    return "finished " + finishedCount.get() + " success " + successCons.size() + " target count:"
        + this.total;
  }

  public boolean finished() {
    return finishedCount.get() >= total;
  }

  @Override
  public void connectionAcquired(BackendConnection conn) {
    successCons.add(conn);
    finishedCount.addAndGet(1);
    logger.info("connected successfuly " + conn);
    conn.release();
  }

  @Override
  public void connectionError(Throwable e, BackendConnection conn) {
    finishedCount.addAndGet(1);
    logger.warn("connect error " + conn + e);
    conn.release();
  }

  @Override
  public void errorResponse(byte[] err, BackendConnection conn) {
    logger.warn("caught error resp: " + conn + " " + new String(err));
    conn.release();
  }

  @Override
  public void okResponse(byte[] ok, BackendConnection conn) {
    logger.info("received ok resp: " + conn + " " + new String(ok));

  }

  @Override
  public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof,
      BackendConnection conn) {

  }

  @Override
  public void rowResponse(byte[] row, BackendConnection conn) {

  }

  @Override
  public void rowEofResponse(byte[] eof, BackendConnection conn) {

  }

  @Override
  public void writeQueueAvailable() {

  }

  @Override
  public void connectionClose(BackendConnection conn, String reason) {

  }

}