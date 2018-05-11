/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route.hint;

import java.util.HashMap;
import java.util.Map;

public class HintHandlerFactory {

  private static volatile boolean isInit = false;

  // sql注释的类型处理handler 集合，现在支持两种类型的处理：sql,schema
  private static Map<String, HintHandler> hintHandlerMap = new HashMap<String, HintHandler>();

  private HintHandlerFactory() {
  }

  private static void init() {
    hintHandlerMap.put("sql", new HintSQLHandler());
    hintHandlerMap.put("schema", new HintSchemaHandler());
//		hintHandlerMap.put("datanode", new HintDataNodeHandler()); 这个没有实现好，暂时关闭
    hintHandlerMap.put("table-index", new HintTableHandler());
    isInit = true; // 修复多次初始化的bug
  }

  // 双重校验锁 fix 线程安全问题
  public static HintHandler getHintHandler(String hintType) {
    if (!isInit) {
      synchronized (HintHandlerFactory.class) {
        if (!isInit) {
          init();
        }
      }
    }
    return hintHandlerMap.get(hintType);
  }

}
