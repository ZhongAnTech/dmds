/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route.factory;

import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.route.RouteStrategy;
import com.zhongan.dmds.route.impl.DruidDmdsRouteStrategy;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 路由策略工厂类
 */
public class RouteStrategyFactory {

  private static RouteStrategy defaultStrategy = null;
  private static volatile boolean isInit = false;
  private static ConcurrentMap<String, RouteStrategy> strategyMap = new ConcurrentHashMap<String, RouteStrategy>();

  private RouteStrategyFactory() {

  }

  private static void init() {
    SystemConfig config = DmdsContext.getInstance().getSystem();
    String defaultSqlParser = config.getDefaultSqlParser();
    defaultSqlParser = defaultSqlParser == null ? "" : defaultSqlParser;
    // 修改为ConcurrentHashMap，避免并发问题
    strategyMap.putIfAbsent("druidparser", new DruidDmdsRouteStrategy());

    defaultStrategy = strategyMap.get(defaultSqlParser);
    if (defaultStrategy == null) {
      defaultStrategy = strategyMap.get("druidparser");
      defaultSqlParser = "druidparser";
    }
    config.setDefaultSqlParser(defaultSqlParser);
    isInit = true;
  }

  public static RouteStrategy getRouteStrategy() {
    if (!isInit) {
      synchronized (RouteStrategyFactory.class) {
        if (!isInit) {
          init();
        }
      }
    }
    return defaultStrategy;
  }

  public static RouteStrategy getRouteStrategy(String parserType) {
    if (!isInit) {
      synchronized (RouteStrategyFactory.class) {
        if (!isInit) {
          init();
        }
      }
    }
    return strategyMap.get(parserType);
  }
}
