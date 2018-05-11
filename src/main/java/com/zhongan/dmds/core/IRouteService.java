/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.zhongan.dmds.cache.LayerCachePool;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.route.RouteResultset;

import java.sql.SQLNonTransientException;

/**
 * Extract from RouteService
 */
public interface IRouteService {

  String DMDS_HINT_TYPE = "_dmdsHintType";

  LayerCachePool getTableId2DataNodeCache();

  RouteResultset route(SystemConfig sysconf, SchemaConfig schema, int sqlType, String stmt,
      String charset,
      IServerConnection sc)
      throws SQLNonTransientException;

}
