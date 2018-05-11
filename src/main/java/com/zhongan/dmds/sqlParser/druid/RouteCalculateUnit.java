/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid;

import com.zhongan.dmds.mpp.ColumnRoutePair;
import com.zhongan.dmds.mpp.RangeValue;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 路由计算单元
 */
public class RouteCalculateUnit {

  private Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = new LinkedHashMap<String, Map<String, Set<ColumnRoutePair>>>();

  public Map<String, Map<String, Set<ColumnRoutePair>>> getTablesAndConditions() {
    return tablesAndConditions;
  }

  public void addShardingExpr(String tableName, String columnName, Object value) {
    Map<String, Set<ColumnRoutePair>> tableColumnsMap = tablesAndConditions.get(tableName);

    if (value == null) {
      // where a=null
      return;
    }

    if (tableColumnsMap == null) {
      tableColumnsMap = new LinkedHashMap<String, Set<ColumnRoutePair>>();
      tablesAndConditions.put(tableName, tableColumnsMap);
    }

    String uperColName = columnName.toUpperCase();
    Set<ColumnRoutePair> columValues = tableColumnsMap.get(uperColName);

    if (columValues == null) {
      columValues = new LinkedHashSet<ColumnRoutePair>();
      tablesAndConditions.get(tableName).put(uperColName, columValues);
    }

    if (value instanceof Object[]) {
      for (Object item : (Object[]) value) {
        if (item == null) {
          continue;
        }
        columValues.add(new ColumnRoutePair(item.toString()));
      }
    } else if (value instanceof RangeValue) {
      columValues.add(new ColumnRoutePair((RangeValue) value));
    } else {
      columValues.add(new ColumnRoutePair(value.toString()));
    }
  }

  public void clear() {
    tablesAndConditions.clear();
  }

}
