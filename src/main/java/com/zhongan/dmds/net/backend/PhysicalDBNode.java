/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.backend;

import com.zhongan.dmds.core.*;
import com.zhongan.dmds.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhysicalDBNode implements IPhysicalDBNode {

  protected static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDBNode.class);

  protected final String name;
  protected final String database;
  protected final IPhysicalDBPool dbPool;

  public PhysicalDBNode(String hostName, String database, IPhysicalDBPool dbPool) {
    this.name = hostName;
    this.database = database;
    this.dbPool = dbPool;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public IPhysicalDBPool getDbPool() {
    return dbPool;
  }

  @Override
  public String getDatabase() {
    return database;
  }

  @Override
  public void getConnectionFromSameSource(String schema, boolean autocommit,
      BackendConnection exitsCon,
      ResponseHandler handler, Object attachment) throws Exception {

    IPhysicalDatasource ds = this.dbPool.findDatasouce(exitsCon);
    if (ds == null) {
      throw new RuntimeException("can't find exits connection,maybe fininshed " + exitsCon);
    } else {
      ds.getConnection(schema, autocommit, handler, attachment);
    }

  }

  private void checkRequest(String schema) {
    if (schema != null && !schema.equals(this.database)) {
      throw new RuntimeException(
          "invalid param ,connection request db is :" + schema + " and datanode db is "
              + this.database);
    }
    if (!dbPool.isInitSuccess()) {
      dbPool.init(dbPool.getActivedIndex());
    }
  }

  @Override
  public void getConnection(String schema, boolean autoCommit, RouteResultsetNode rrs,
      ResponseHandler handler,
      Object attachment) throws Exception {
    checkRequest(schema);
    if (dbPool.isInitSuccess()) {
      if (rrs.canRunnINReadDB(autoCommit)) {
        dbPool.getRWBanlanceCon(schema, autoCommit, handler, attachment, this.database);
      } else {
        dbPool.getSource().getConnection(schema, autoCommit, handler, attachment);
      }

    } else {
      throw new IllegalArgumentException("Invalid DataSource:" + dbPool.getActivedIndex());
    }
  }
}