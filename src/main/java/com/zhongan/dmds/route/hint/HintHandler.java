/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route.hint;

import com.zhongan.dmds.cache.LayerCachePool;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.route.RouteResultset;

import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * 按照注释中包含指定类型的内容做路由解析
 */
public interface HintHandler {

  public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema,
      int sqlType, String realSQL, String charset, IServerConnection sc,
      LayerCachePool cachePool, String hintSQLValue, int hintSqlType, Map hintMap)
      throws SQLNonTransientException;
}
