/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server;

import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.commons.util.TimeUtil;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.net.frontend.FrontendConnection;
import com.zhongan.dmds.net.session.NonBlockingSession;
import com.zhongan.dmds.net.util.SchemaUtil;
import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.server.cmd.Heartbeat;
import com.zhongan.dmds.server.cmd.Ping;
import com.zhongan.dmds.server.handler.MysqlProcHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

public class ServerConnection extends FrontendConnection implements IServerConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerConnection.class);
  ;
  private static final long AUTH_TIMEOUT = 15 * 1000L;

  private volatile int txIsolation;
  private volatile boolean autocommit;
  private volatile boolean txInterrupted;
  private volatile String txInterrputMsg = "";
  private long lastInsertId;
  private NonBlockingSession session;

  public ServerConnection(NetworkChannel channel) throws IOException {
    super(channel);
    this.txInterrupted = false;
    this.autocommit = true;
  }

  @Override
  public boolean isIdleTimeout() {
    if (isAuthenticated) {
      return super.isIdleTimeout();
    } else {
      return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + AUTH_TIMEOUT;
    }
  }

  public int getTxIsolation() {
    return txIsolation;
  }

  public void setTxIsolation(int txIsolation) {
    this.txIsolation = txIsolation;
  }

  public boolean isAutocommit() {
    return autocommit;
  }

  public void setAutocommit(boolean autocommit) {
    this.autocommit = autocommit;
  }

  public long getLastInsertId() {
    return lastInsertId;
  }

  public void setLastInsertId(long lastInsertId) {
    this.lastInsertId = lastInsertId;
  }

  /**
   * 设置是否需要中断当前事务
   */
  public void setTxInterrupt(String txInterrputMsg) {
    if (!autocommit && !txInterrupted) {
      txInterrupted = true;
      this.txInterrputMsg = txInterrputMsg;
    }
  }

  public boolean isTxInterrupted() {
    return txInterrupted;
  }

  public NonBlockingSession getSession2() {
    return session;
  }

  public void setSession2(NonBlockingSession session2) {
    this.session = session2;
  }

  @Override
  public void ping() {
    Ping.response(this);
  }

  @Override
  public void heartbeat(byte[] data) {
    Heartbeat.response(this, data);
  }

  public void execute(String sql, int type) {
    if (this.isClosed()) {
      LOGGER.warn("ignore execute ,server connection is closed " + this);
      return;
    }
    // 状态检查
    if (txInterrupted) {
      writeErrMessage(ErrorCode.ER_YES, "Transaction error, need to rollback." + txInterrputMsg);
      return;
    }

    // 检查当前使用的DB
    String db = this.schema;
    if (db == null) {
      db = SchemaUtil.detectDefaultDb(sql, type);
      if (db == null) {
        writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No DMDS Database selected");
        return;
      }
    }

    if (ServerParse.SELECT == type && sql.contains("mysql") && sql.contains("proc")) {
      SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
      if (schemaInfo != null && "mysql".equalsIgnoreCase(schemaInfo.schema)
          && "proc".equalsIgnoreCase(schemaInfo.table)) {
        // 兼容MySQLWorkbench
        MysqlProcHandler.handle(sql, this);
        return;
      }
    }
    SchemaConfig schema = DmdsContext.getInstance().getConfig().getSchemas().get(db);
    if (schema == null) {
      writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown DMDS Database '" + db + "'");
      return;
    }

    routeEndExecuteSQL(sql, type, schema);

  }

  public RouteResultset routeSQL(String sql, int type) {
    // 检查当前使用的DB
    String db = this.schema;
    if (db == null) {
      writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No DMDS Database selected");
      return null;
    }
    SchemaConfig schema = DmdsContext.getInstance().getConfig().getSchemas().get(db);
    if (schema == null) {
      writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown DMDS Database '" + db + "'");
      return null;
    }
    // 路由计算
    RouteResultset rrs = null;
    try {
      rrs = DmdsContext.getInstance().getRouterservice()
          .route(DmdsContext.getInstance().getSystem(), schema,
              type, sql, this.charset, this);
    } catch (Exception e) {
      StringBuilder s = new StringBuilder();
      LOGGER.warn(s.append(this).append(sql).toString() + " err:" + e.toString(), e);
      String msg = e.getMessage();
      writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
      return null;
    }
    return rrs;
  }

  public void routeEndExecuteSQL(String sql, int type, SchemaConfig schema) {
    // 路由计算
    RouteResultset rrs = null;
    try {
      rrs = DmdsContext.getInstance().getRouterservice()
          .route(DmdsContext.getInstance().getSystem(), schema,
              type, sql, this.charset, this);
    } catch (Throwable e) {
      StringBuilder s = new StringBuilder();
      LOGGER.warn(s.append(this).append(sql).toString() + " err:" + e.toString(), e);
      String msg = e.getMessage();
      writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
      return;
    }
    if (rrs != null) {
      // session执行
      session.execute(rrs, type);
    }
  }

  /**
   * 提交事务
   */
  public void commit() {
    if (txInterrupted) {
      writeErrMessage(ErrorCode.ER_YES, "Transaction error, need to rollback.");
    } else {
      session.commit();
    }
  }

  /**
   * 回滚事务
   */
  public void rollback() {
    // 状态检查
    if (txInterrupted) {
      txInterrupted = false;
    }

    // 执行回滚
    session.rollback();
  }

  /**
   * 撤销执行中的语句
   *
   * @param sponsor 发起者为null表示是自己
   */
  public void cancel(final FrontendConnection sponsor) {
    processor.getExecutor().execute(new Runnable() {
      @Override
      public void run() {
        session.cancel(sponsor);
      }
    });
  }

  @Override
  public void close(String reason) {
    super.close(reason);
    session.terminate();
    if (getLoadDataInfileHandler() != null) {
      getLoadDataInfileHandler().clear();
    }
  }

  @Override
  public String toString() {
    return "ServerConnection [id=" + id + ", schema=" + schema + ", host=" + host + ", user=" + user
        + ",txIsolation=" + txIsolation + ", autocommit=" + autocommit + ", schema=" + schema + "]";
  }

  /*
   * (non-Javadoc)
   *
   * @see com.zhongan.dmds.core.NIOConnection#setHandler(com.zhongan.dmds.core.
   * NIOHandler)
   */

}