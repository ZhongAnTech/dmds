/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.sqlengine;

import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.core.*;
import com.zhongan.dmds.net.backend.PhysicalDatasource;
import com.zhongan.dmds.net.protocol.ErrorPacket;
import com.zhongan.dmds.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * asyn execute in EngineCtx or standalone (EngineCtx=null)
 */
public class SQLJob implements ResponseHandler, Runnable {

  public static final Logger LOGGER = LoggerFactory.getLogger(SQLJob.class);
  private final String sql;
  private final String dataNodeOrDatabase;
  private BackendConnection connection;
  private final SQLJobHandler jobHandler;
  private final EngineCtx ctx;
  private final PhysicalDatasource ds;
  private final int id;
  private volatile boolean finished;

  public SQLJob(int id, String sql, String dataNode, SQLJobHandler jobHandler, EngineCtx ctx) {
    super();
    this.id = id;
    this.sql = sql;
    this.dataNodeOrDatabase = dataNode;
    this.jobHandler = jobHandler;
    this.ctx = ctx;
    this.ds = null;
  }

  public SQLJob(String sql, String databaseName, SQLJobHandler jobHandler, PhysicalDatasource ds) {
    super();
    this.id = 0;
    this.sql = sql;
    this.dataNodeOrDatabase = databaseName;
    this.jobHandler = jobHandler;
    this.ctx = null;
    this.ds = ds;

  }

  public void run() {
    try {
      if (ds == null) {
        RouteResultsetNode node = new RouteResultsetNode(dataNodeOrDatabase, ServerParse.SELECT,
            sql);
        // create new connection
        IDmdsConfig conf = DmdsContext.getInstance().getConfig();
        IPhysicalDBNode dn = conf.getDataNodes().get(node.getName());
        dn.getConnection(dn.getDatabase(), true, node, this, node);
      } else {
        ds.getConnection(dataNodeOrDatabase, true, this, null);
      }
    } catch (Exception e) {
      LOGGER.error("can't get connection for sql ,error:" + e);
      doFinished(true);
    }
  }

  public void teminate(String reason) {
    LOGGER.info("terminate this job reason:" + reason + " con:" + connection + " sql " + this.sql);
    if (connection != null) {
      connection.close(reason);
    }
  }

  @Override
  public void connectionAcquired(final BackendConnection conn) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("con query sql:" + sql + " to con:" + conn);
    }
    conn.setResponseHandler(this);
    try {
      conn.query(sql);
      connection = conn;
    } catch (Exception e) {
      doFinished(true);
    }
  }

  public boolean isFinished() {
    return finished;
  }

  private void doFinished(boolean failed) {
    finished = true;
    jobHandler.finished(dataNodeOrDatabase, failed);
    if (ctx != null) {
      ctx.onJobFinished(this);
    }
  }

  @Override
  public void connectionError(Throwable e, BackendConnection conn) {
    LOGGER.info("can't get connection for sql :" + sql);
    doFinished(true);

  }

  @Override
  public void errorResponse(byte[] err, BackendConnection conn) {
    ErrorPacket errPg = new ErrorPacket();
    errPg.read(err);
    LOGGER.info(
        "error response " + new String(errPg.message) + " from of sql :" + sql + " at con:" + conn);
    conn.release();
    doFinished(true);

  }

  @Override
  public void okResponse(byte[] ok, BackendConnection conn) {
    conn.syncAndExcute();
  }

  @Override
  public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof,
      BackendConnection conn) {
    jobHandler.onHeader(dataNodeOrDatabase, header, fields);

  }

  @Override
  public void rowResponse(byte[] row, BackendConnection conn) {
    boolean finsihed = jobHandler.onRowData(dataNodeOrDatabase, row);
    if (finsihed) {
      conn.close("not needed by user proc");
      doFinished(false);
    }

  }

  @Override
  public void rowEofResponse(byte[] eof, BackendConnection conn) {
    conn.release();
    doFinished(false);
  }

  @Override
  public void writeQueueAvailable() {

  }

  @Override
  public void connectionClose(BackendConnection conn, String reason) {
    doFinished(true);
  }

  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return "SQLJob [ id=" + id + ",dataNodeOrDatabase=" + dataNodeOrDatabase + ",sql=" + sql
        + ",  jobHandler="
        + jobHandler + "]";
  }

}
