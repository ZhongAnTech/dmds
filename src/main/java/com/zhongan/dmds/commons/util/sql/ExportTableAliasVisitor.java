/*
 * Copyright (C) 2016-2020 zhongan.com
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.util.sql;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.HashMap;
import java.util.Map;

/**
 * Druid visitor for get sql table alias Druid version 1.0.14
 */
public class ExportTableAliasVisitor extends MySqlASTVisitorAdapter {

  private Map<String, SQLTableSource> aliasMap = new HashMap<String, SQLTableSource>();

  public boolean visit(SQLExprTableSource x) {
    if (x.getAlias() != null) {
      aliasMap.put(x.getAlias(), x);
    }
    return true;
  }

  public Map<String, SQLTableSource> getAliasMap() {
    return aliasMap;
  }
}
