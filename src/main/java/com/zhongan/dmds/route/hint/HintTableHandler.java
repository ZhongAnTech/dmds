/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route.hint;

import com.zhongan.dmds.cache.LayerCachePool;
import com.zhongan.dmds.commons.contants.mysql.DmdsConstants;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.config.model.TableConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IPhysicalDBNode;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.route.RouteResultsetNode;
import com.zhongan.dmds.route.RouteStrategy;
import com.zhongan.dmds.route.factory.RouteStrategyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.Map;

/**
 * 处理注释中类型为table 的情况
 * Base on 1.6 HintTableHandler
 */
public class HintTableHandler implements HintHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(HintTableHandler.class);

  private RouteStrategy routeStrategy;

  public HintTableHandler() {
    this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
  }


  @Override
  public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema, int sqlType,
      String realSQL,
      String charset, IServerConnection sc, LayerCachePool cachePool, String hintSQLValue,
      int hintSqlType, Map hintMap) throws SQLNonTransientException {
    String stmt = realSQL;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("route table sql hint from " + stmt);
    }
    RouteResultset rrs = routeStrategy
        .route(sysConfig, schema, hintSqlType, stmt, charset, sc, cachePool);
    // 替换RRS中的SQL执行
    RouteResultsetNode[] oldRsNodes = rrs.getNodes();
    RouteResultsetNode[] newRrsNodes = new RouteResultsetNode[1];
    if (oldRsNodes != null && oldRsNodes.length > 0) {
      String tableName = oldRsNodes[0].getTableName();
      String logicTableName = tableName.substring(0, tableName.length() - 5);

      TableConfig tableConfig = schema.getTables().get(logicTableName);
      int tbCount = tableConfig.getRule().getRuleAlgorithm().getTbCount();
      int tableIndex = Integer.valueOf(hintSQLValue);
      int dataNodeIndex = tableIndex / tbCount;

      String dataNodeName = tableConfig.getDataNodes().get(dataNodeIndex);
      IPhysicalDBNode dataNode = DmdsContext.getInstance().getConfig().getDataNodes()
          .get(dataNodeName);

      StringBuilder name = new StringBuilder();
      name.append(dataNode.getName());
      name.append(DmdsConstants.seq_db_tb);
      name.append(oldRsNodes[0].getTableName());
      String oldSql = oldRsNodes[0].getStatement();
      String newSql = oldSql.replaceAll(logicTableName.toLowerCase() + "(?i)_\\d{4}",
          logicTableName + "_" + hintSQLValue);

      newRrsNodes[0] = new RouteResultsetNode(name.toString(), oldRsNodes[0].getSqlType(), newSql);
      rrs.setNodes(newRrsNodes);
    } else {
      String msg = "can't find hint datanode:" + hintSQLValue;
      LOGGER.warn(msg);
      throw new SQLNonTransientException(msg);
    }
    return rrs;
  }

}
