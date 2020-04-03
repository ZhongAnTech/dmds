/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route.hint;

import com.zhongan.dmds.cache.LayerCachePool;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.route.RouteStrategy;
import com.zhongan.dmds.route.factory.RouteStrategyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * 处理注释中类型为schema 的情况（按照指定schema做路由解析）
 */
public class HintSchemaHandler implements HintHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(HintSchemaHandler.class);

  private RouteStrategy routeStrategy;

  public HintSchemaHandler() {
    this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
  }

  /**
   * 从全局的schema列表中查询指定的schema是否存在， 如果存在则替换connection属性中原有的schema， 如果不存在，则throws
   * SQLNonTransientException，表示指定的schema 不存在
   *
   * @param sysConfig
   * @param schema
   * @param sqlType
   * @param realSQL
   * @param charset
   * @param info
   * @param cachePool
   * @param hintSQLValue
   * @return
   * @throws SQLNonTransientException
   */
  @Override
  public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema, int sqlType,
      String realSQL,
      String charset, IServerConnection sc, LayerCachePool cachePool, String hintSQLValue,
      int hintSqlType,
      Map hintMap) throws SQLNonTransientException {
    SchemaConfig tempSchema = DmdsContext.getInstance().getConfig().getSchemas().get(hintSQLValue);
    if (tempSchema != null) {
      return routeStrategy.route(sysConfig, tempSchema, sqlType, realSQL, charset, sc, cachePool);
    } else {
      String msg = "can't find hint schema:" + hintSQLValue;
      LOGGER.warn(msg);
      throw new SQLNonTransientException(msg);
    }
  }
}
