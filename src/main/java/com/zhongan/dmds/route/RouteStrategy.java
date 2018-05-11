/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route;

import com.zhongan.dmds.cache.LayerCachePool;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.core.IServerConnection;

import java.sql.SQLNonTransientException;

/**
 * 路由策略接口
 */
public interface RouteStrategy {

  public RouteResultset route(SystemConfig sysConfig,
      SchemaConfig schema, int sqlType, String origSQL, String charset, IServerConnection sc,
      LayerCachePool cachePool)
      throws SQLNonTransientException;
}
