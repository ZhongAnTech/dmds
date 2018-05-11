/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.stat;

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

public class SQLParserHigh {

  public String fixSql(String sql) {
    if (sql != null) {
      return sql.replace("\n", " ");
    }
    return sql;
  }

  public String mergeSql(String sql) {
    String newSql = ParameterizedOutputVisitorUtils.parameterize(sql, "mysql");
    return fixSql(newSql);
  }

}
