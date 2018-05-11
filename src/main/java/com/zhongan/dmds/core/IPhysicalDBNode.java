/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.zhongan.dmds.route.RouteResultsetNode;

/**
 * Extract from PhysicalDBNode
 */
public interface IPhysicalDBNode {

  String getName();

  IPhysicalDBPool getDbPool();

  String getDatabase();

  /**
   * get connection from the same datasource
   *
   * @param exitsCon
   * @throws Exception
   */
  void getConnectionFromSameSource(String schema, boolean autocommit, BackendConnection exitsCon,
      ResponseHandler handler, Object attachment)
      throws Exception;

  void getConnection(String schema, boolean autoCommit, RouteResultsetNode rrs,
      ResponseHandler handler,
      Object attachment)
      throws Exception;

}
