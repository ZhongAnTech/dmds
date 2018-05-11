/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio;

import com.zhongan.dmds.config.model.DBHostConfig;
import com.zhongan.dmds.config.model.DataHostConfig;
import com.zhongan.dmds.core.IDBHeartbeat;
import com.zhongan.dmds.core.ResponseHandler;
import com.zhongan.dmds.net.backend.PhysicalDatasource;
import com.zhongan.dmds.net.heart.MySQLHeartbeat;

import java.io.IOException;

public class MySQLDataSource extends PhysicalDatasource {

  private final MySQLConnectionFactory factory;

  public MySQLDataSource(DBHostConfig config, DataHostConfig hostConfig, boolean isReadNode) {
    super(config, hostConfig, isReadNode);
    this.factory = new MySQLConnectionFactory();

  }

  @Override
  public void createNewConnection(ResponseHandler handler, String schema) throws IOException {
    factory.make(this, handler, schema);
  }

  @Override
  public IDBHeartbeat createHeartBeat() {
    return new MySQLHeartbeat(this);
  }
}