/*
 * Copyright (C) 2016-2020 zhongan.com
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.util.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Replace sql table name , modify source code from SQLUtils.toSQLString()
 * <p>
 * Druid version 1.0.14
 */
public class MySQLTableReplaceUtil {

  /**
   * Replace sql table name
   *
   * @param sql      source sql
   * @param tableMap table map [source table name : target table name]*
   * @return target sql
   */
  public static String replace(String sql, Map<String, String> tableMap) {
    return replace(sql, tableMap, true, null);
  }


  /**
   * Replace sql table name
   *
   * @param sql               source sql
   * @param tableMap          table map [source table name : target table name]*
   * @param mappingIgnoreCase whether mapping key ignore case. if true , then source->target is the
   *                          same with SOURCE->target
   * @param parameters        SQLASTOutputVisitor parameters
   * @return target sql
   */
  public static String replace(String sql, Map<String, String> tableMap,
      boolean mappingIgnoreCase, List<Object> parameters) {
    if (sql == null || sql.trim().isEmpty()) {
      return sql;
    }
    List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
    List<Collection<String>> aliasTablesList = getAliasTablesList(statementList);
    StringBuilder out = new StringBuilder();
    MySQLTableMapSQLASTOutputVisitor visitor = new MySQLTableMapSQLASTOutputVisitor(out, tableMap,
        mappingIgnoreCase);
    visitor.setPrettyFormat(false);
    if (parameters != null) {
      visitor.setParameters(parameters);
    }

    for (int i = 0; i < statementList.size(); i++) {
      visitor.resetTableAlias(aliasTablesList.get(i));
      if (i > 0) {
        out.append(";\n");
      }
      statementList.get(i).accept(visitor);
    }

    return out.toString();
  }

  private static List<Collection<String>> getAliasTablesList(List<SQLStatement> statementList) {
    List<Collection<String>> aliasTablesList = new LinkedList<Collection<String>>();
    for (int i = 0; i < statementList.size(); i++) {
      SQLStatement stmt = statementList.get(i);
      ExportTableAliasVisitor exportTableAliasVisitor = new ExportTableAliasVisitor();
      stmt.accept(exportTableAliasVisitor);
      aliasTablesList.add(exportTableAliasVisitor.getAliasMap().keySet());
    }
    return aliasTablesList;
  }
}
