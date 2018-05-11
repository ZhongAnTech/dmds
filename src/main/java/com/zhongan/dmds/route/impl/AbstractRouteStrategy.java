/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route.impl;

import com.zhongan.dmds.cache.LayerCachePool;
import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.mpp.LoadData;
import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.route.RouteStrategy;
import com.zhongan.dmds.route.RouterUtil;
import com.zhongan.dmds.sqlParser.util.DmdsRouterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;

/**
 * 2017.02 move interceptSQL from RouteService to here for more intercept
 */
public abstract class AbstractRouteStrategy implements RouteStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRouteStrategy.class);

  @Override
  public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema, int sqlType,
      String origSQL,
      String charset, IServerConnection sc, LayerCachePool cachePool)
      throws SQLNonTransientException {

    /**
     * 处理一些路由之前的逻辑
     */
    if (beforeRouteProcess(schema, sqlType, origSQL, sc)) {
      return null;
    }

    /**
     * SQL 语句拦截
     */
    // String stmt =
    // DmdsContext.getInstance().getSqlInterceptor().interceptSQL(origSQL, sqlType);
    // if (origSQL != stmt && LOGGER.isDebugEnabled()) {
    // LOGGER.debug("sql intercepted to " + stmt + " from " + origSQL);
    // }
    String stmt = origSQL;

    if (schema.isCheckSQLSchema()) {
      stmt = DmdsRouterUtil.removeSchema(stmt, schema.getName());
    }

    RouteResultset rrs = new RouteResultset(stmt, sqlType);

    /**
     * 优化debug loaddata输出cache的日志会极大降低性能
     */
    if (LOGGER.isDebugEnabled() && origSQL.startsWith(LoadData.loadDataHint)) {
      rrs.setCacheAble(false);
    }

    /**
     * rrs携带ServerConnection的autocommit状态用于在sql解析的时候遇到 select ... for
     * update的时候动态设定RouteResultsetNode的canRunInReadDB属性
     */
    if (sc != null) {
      rrs.setAutocommit(sc.isAutocommit());
    }

    /**
     * DDL 语句的路由
     */
    if (ServerParse.DDL == sqlType) {
      return DmdsRouterUtil.routeToDDLNode(rrs, sqlType, stmt, schema);
    }

    /**
     * 检查是否有分片
     */
    if (schema.isNoSharding() && ServerParse.SHOW != sqlType) {
      rrs = DmdsRouterUtil.routeToSingleNode(rrs, schema.getDataNode(), stmt);
    } else {

      RouteResultset returnedSet = routeSystemInfo(schema, sqlType, stmt, rrs);
      if (returnedSet == null) {
        rrs = routeNormalSqlWithAST(schema, stmt, rrs, charset, cachePool);
      }
    }

    return rrs;
  }

  /**
   * 路由之前必要的处理
   */
  private boolean beforeRouteProcess(SchemaConfig schema, int sqlType, String origSQL,
      IServerConnection sc)
      throws SQLNonTransientException {

    return sqlType == ServerParse.INSERT && RouterUtil.processInsert(schema, sqlType, origSQL, sc);
  }

  /**
   * 通过解析AST语法树类来寻找路由
   */
  public abstract RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String stmt,
      RouteResultset rrs,
      String charset, LayerCachePool cachePool) throws SQLNonTransientException;

  /**
   * 路由信息指令, 如 SHOW、SELECT@@、DESCRIBE
   */
  public abstract RouteResultset routeSystemInfo(SchemaConfig schema, int sqlType, String stmt,
      RouteResultset rrs)
      throws SQLSyntaxErrorException;

  /**
   * 解析 Show 之类的语句
   */
  public abstract RouteResultset analyseShowSQL(SchemaConfig schema, RouteResultset rrs,
      String stmt)
      throws SQLNonTransientException;

}
