/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.jdbc;

import com.google.common.collect.Lists;
import com.zhongan.dmds.config.model.DBHostConfig;
import com.zhongan.dmds.config.model.DataHostConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IDBHeartbeat;
import com.zhongan.dmds.core.ResponseHandler;
import com.zhongan.dmds.net.NIOConnector;
import com.zhongan.dmds.net.NIOProcessor;
import com.zhongan.dmds.net.backend.PhysicalDatasource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * 2017.04 移除多其他数据库的支持，专注于MySQL
 */
public class JDBCDatasource extends PhysicalDatasource {

  static {
    // 加载可能的驱动
    List<String> drivers = Lists.newArrayList("com.mysql.jdbc.Driver");
    for (String driver : drivers) {
      try {
        Class.forName(driver);
      } catch (ClassNotFoundException ignored) {
      }
    }
  }

  public JDBCDatasource(DBHostConfig config, DataHostConfig hostConfig, boolean isReadNode) {
    super(config, hostConfig, isReadNode);
  }

  @Override
  public IDBHeartbeat createHeartBeat() {
    return new JDBCHeartbeat(this);
  }

  @Override
  public void createNewConnection(ResponseHandler handler, String schema) throws IOException {
    DBHostConfig cfg = getConfig();
    JDBCConnection c = new JDBCConnection();

    c.setHost(cfg.getIp());
    c.setPort(cfg.getPort());
    c.setPool(this);
    c.setSchema(schema);
    c.setDbType(cfg.getDbType());

    NIOProcessor processor = (NIOProcessor) DmdsContext.getInstance().nextProcessor();
    c.setProcessor(processor);
    c.setId(NIOConnector.ID_GENERATOR.getId()); // 复用mysql的Backend的ID，需要在process中存储

    processor.addBackend(c);
    try {
      Connection con = getConnection();
      // c.setIdleTimeout(pool.getConfig().getIdleTimeout());
      c.setCon(con);
      // notify handler
      handler.connectionAcquired(c);
    } catch (Exception e) {
      handler.connectionError(e, c);
    }
  }

  Connection getConnection() throws SQLException {
    DBHostConfig cfg = getConfig();
    Connection connection = DriverManager
        .getConnection(cfg.getUrl(), cfg.getUser(), cfg.getPassword());
    String initSql = getHostConfig().getConnectionInitSql();
    if (initSql != null && !"".equals(initSql)) {
      Statement statement = null;
      try {
        statement = connection.createStatement();
        statement.execute(initSql);
      } finally {
        if (statement != null) {
          statement.close();
        }
      }
    }
    return connection;
  }
}
