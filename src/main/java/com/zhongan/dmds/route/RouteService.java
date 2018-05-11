/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route;

import com.zhongan.dmds.cache.CachePool;
import com.zhongan.dmds.cache.CacheService;
import com.zhongan.dmds.cache.LayerCachePool;
import com.zhongan.dmds.commons.contants.mysql.DmdsConstants;
import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IRouteService;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.route.factory.RouteStrategyFactory;
import com.zhongan.dmds.route.hint.HintHandler;
import com.zhongan.dmds.route.hint.HintHandlerFactory;
import com.zhongan.dmds.route.hint.HintSQLHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

public class RouteService implements IRouteService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RouteService.class);
  private final CachePool sqlRouteCache;
  private final LayerCachePool tableId2DataNodeCache;

  public RouteService(CacheService cachService) {
    sqlRouteCache = cachService.getCachePool("SQLRouteCache");
    tableId2DataNodeCache = (LayerCachePool) cachService.getCachePool("TableID2DataNodeCache");
  }

  @Override
  public LayerCachePool getTableId2DataNodeCache() {
    return tableId2DataNodeCache;
  }

  @Override
  public RouteResultset route(SystemConfig sysconf, SchemaConfig schema, int sqlType, String stmt,
      String charset,
      IServerConnection sc) throws SQLNonTransientException {
    RouteResultset rrs = null;
    String cacheKey = null;

    /**
     * SQL 语句拦截
     */
    String stmt2 = DmdsContext.getInstance().getSqlInterceptor().interceptSQL(stmt, sqlType, sc);
    if (stmt2 != stmt && LOGGER.isDebugEnabled()) {
      LOGGER.debug("sql intercepted to " + stmt2 + " from " + stmt);
      stmt = stmt2;
    }

    /**
     * SELECT 类型的SQL, 检测
     */
    if (sqlType == ServerParse.SELECT) {
      cacheKey = schema.getName() + stmt;
      rrs = (RouteResultset) sqlRouteCache.get(cacheKey);
      if (rrs != null) {
        return rrs;
      }
    }

    boolean isMatchHint = stmt.startsWith(DmdsConstants.HINT);
    if (isMatchHint) {
      int endPos = stmt.indexOf("*/");
      if (endPos > 0) {
        // 用!dmds:内部的语句来做路由分析
        int hintLength = DmdsConstants.HINT.length();
        String hint = stmt.substring(hintLength, endPos).trim();
        int firstSplitPos = hint.indexOf(DmdsConstants.HINT_SPLIT);
        if (firstSplitPos > 0) {
          Map<String, String> hintMap = parseHint(hint);
          String hintType = (String) hintMap.get(DMDS_HINT_TYPE);
          String hintSql = (String) hintMap.get(hintType);
          if (hintSql.length() == 0) {
            LOGGER.warn("comment int sql must meet :/*!dmds:type=value*/: " + stmt);
            throw new SQLSyntaxErrorException(
                "comment int sql must meet :/*!dmds:type=value*/: " + stmt);
          }
          String realSQL = stmt.substring(endPos + "*/".length()).trim();

          HintHandler hintHandler = HintHandlerFactory.getHintHandler(hintType);
          if (hintHandler != null) {

            if (hintHandler instanceof HintSQLHandler) {
              /**
               * 修复 注解SQL的 sqlType 与 实际SQL的 sqlType 不一致问题， 如： hint=SELECT，real=INSERT
               */
              int hintSqlType = ServerParse.parse(hintSql) & 0xff;
              rrs = hintHandler.route(sysconf, schema, sqlType, realSQL, charset, sc,
                  tableId2DataNodeCache, hintSql, hintSqlType, hintMap);

            } else {
              rrs = hintHandler.route(sysconf, schema, sqlType, realSQL, charset, sc,
                  tableId2DataNodeCache, hintSql, sqlType, hintMap);
            }

          } else {
            LOGGER.warn("TODO , support hint sql type : " + hintType);
          }

        } else {
          LOGGER.warn("comment in sql must meet :/*!dmds:type=value*/: " + stmt);
          throw new SQLSyntaxErrorException(
              "comment in sql must meet :/*!dmds:type=value*/: " + stmt);
        }
      }
    } else {
      stmt = stmt.trim();
      // modified by niuzichun
      // 替换掉DDL语句中含有"`"符号
      // stmt = stmt.replaceAll("`", "");
      rrs = RouteStrategyFactory.getRouteStrategy()
          .route(sysconf, schema, sqlType, stmt, charset, sc,
              tableId2DataNodeCache);

//			if (rrs != null && sqlType == ServerParse.SELECT && rrs.getNodes().length > 1) {
//				throw new IllegalArgumentException("SQL语句没有包含分片字段");
//			}
    }

    if (rrs != null && sqlType == ServerParse.SELECT && rrs.isCacheAble()) {
      if (rrs.getNodes().length == 0) {
        sqlRouteCache.putIfAbsent(cacheKey, rrs);
      }
    }
    if (rrs != null) {
      LOGGER.debug(rrs.toString());
    }
    return rrs;
  }

  private Map<String, String> parseHint(String sql) {
    Map<String, String> map = new HashMap<>();
    int y = 0;
    int begin = 0;
    for (int i = 0; i < sql.length(); i++) {
      char cur = sql.charAt(i);
      if (cur == ',' && y % 2 == 0) {
        String substring = sql.substring(begin, i);
        parseKeyValue(map, substring);
        begin = i + 1;
      } else if (cur == '\'') {
        y++;
      }
      if (i == sql.length() - 1) {
        parseKeyValue(map, sql.substring(begin));
      }
    }
    return map;
  }

  private void parseKeyValue(Map<String, String> map, String substring) {
    int indexOf = substring.indexOf('=');
    if (indexOf != -1) {
      String key = substring.substring(0, indexOf).trim().toLowerCase();
      String value = substring.substring(indexOf + 1, substring.length());
      if (value.endsWith("'") && value.startsWith("'")) {
        value = value.substring(1, value.length() - 1);
      }
      if (map.isEmpty()) {
        map.put(DMDS_HINT_TYPE, key);
      }
      map.put(key, value.trim());
    }
  }

}
