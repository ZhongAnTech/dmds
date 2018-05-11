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
 * 处理注释中类型为datanode 的情况
 * 2016.12 dmds支持分表
 */
public class HintDataNodeHandler implements HintHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(HintSchemaHandler.class);

  private RouteStrategy routeStrategy;

  public HintDataNodeHandler() {
    this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
  }

  @Override
  public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema, int sqlType,
      String realSQL,
      String charset, IServerConnection sc, LayerCachePool cachePool, String hintSQLValue,
      int hintSqlType,
      Map hintMap) throws SQLNonTransientException {

    String stmt = realSQL;

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("route datanode sql hint from " + stmt);
    }

    // modify by zhongan
    IPhysicalDBNode dataNode = DmdsContext.getInstance().getConfig().getDataNodes()
        .get(hintSQLValue);
    RouteResultset rrs = null;
    if (dataNode != null) {
      rrs = routeStrategy.route(sysConfig, schema, hintSqlType, stmt, charset, sc, cachePool);
      // 替换RRS中的SQL执行
      RouteResultsetNode[] oldRsNodes = rrs.getNodes();
      RouteResultsetNode[] newRrsNodes = new RouteResultsetNode[oldRsNodes.length];
      for (int i = 0; i < newRrsNodes.length; i++) {
        StringBuilder name = new StringBuilder();
        name.append(dataNode.getName());
        name.append(DmdsConstants.seq_db_tb);
        name.append(oldRsNodes[i].getTableName());
        newRrsNodes[i] = new RouteResultsetNode(name.toString(), oldRsNodes[i].getSqlType(),
            realSQL);
      }
      rrs.setNodes(newRrsNodes);
    } else {
      String msg = "can't find hint datanode:" + hintSQLValue;
      LOGGER.warn(msg);
      throw new SQLNonTransientException(msg);
    }
    return rrs;
  }

}
