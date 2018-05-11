/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model;

import java.util.HashMap;

/**
 * 支持表名中包含引号[`]
 *
 * @author BEN GONG
 */
public class TableConfigMap extends HashMap<String, TableConfig> {

  private static final long serialVersionUID = -6605226933829917213L;

  @Override
  public TableConfig get(Object key) {
    String tableName = key.toString();
    // 忽略表名中的引号。
    if (tableName.contains("`")) {
      tableName = tableName.replaceAll("`", "");
    }

    return super.get(tableName);
  }

  @Override
  public boolean containsKey(Object key) {
    String tableName = key.toString();
    // 忽略表名中的引号。
    if (tableName.contains("`")) {
      tableName = tableName.replaceAll("`", "");
    }

    return super.containsKey(tableName);
  }
}
