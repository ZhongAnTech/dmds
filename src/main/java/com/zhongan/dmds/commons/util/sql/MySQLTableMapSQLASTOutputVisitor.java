/*
 * Copyright (C) 2016-2020 zhongan.com
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.util.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLCreateTriggerStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * A visitor base on MySqlOutputVisitor Can set table map to replace table
 * <p>
 * Druid version 1.0.14
 */
public class MySQLTableMapSQLASTOutputVisitor extends MySqlOutputVisitor {

  private Map<String, String> tableMap;

  private boolean ignoreCase;

  private Collection<String> tableAlias = new HashSet<String>();

  private static final Character BACK_QOUTE = '`';

  private static final Character WHITESPACE_CHAR = ' ';

  public MySQLTableMapSQLASTOutputVisitor(Appendable appender,
      Map<String, String> tableMap, boolean ignoreCase) {
    super(appender);
    this.ignoreCase = ignoreCase;
    this.tableMap = tableMap;
    if (ignoreCase) {
      this.tableMap = new HashMap<String, String>();
      for (String key : tableMap.keySet()) {
        this.tableMap.put(key == null ? null : key.toUpperCase(), tableMap.get(key));
      }
    }
  }

  public void resetTableAlias(Collection<String> tableAlias) {
    this.tableAlias = tableAlias;
  }

  @Override
  public boolean visit(SQLIdentifierExpr x) {
    printTable(shouldReplace(x), x.getName());
    return false;
  }

  @Override
  public boolean visit(SQLPropertyExpr x) {
    x.getOwner().accept(this);
    print(".");
    printTable(shouldReplace(x), x.getName());
    return false;
  }

  @Override
  public boolean visit(MySqlRenameTableStatement.Item x) {
    tableVisit(x.getName());
    print(" TO ");
    x.getTo().accept(this);
    return false;
  }

  @Override
  public boolean visit(SQLCreateTriggerStatement x) {
    print("CREATE ");

    if (x.isOrReplace()) {
      print("OR REPLEACE ");
    }

    print("TRIGGER ");

    x.getName().accept(this);

    incrementIndent();
    println();
    if (SQLCreateTriggerStatement.TriggerType.INSTEAD_OF.equals(x.getTriggerType())) {
      print("INSTEAD OF");
    } else {
      print(x.getTriggerType().name());
    }

    for (SQLCreateTriggerStatement.TriggerEvent event : x.getTriggerEvents()) {
      print(' ');
      print(event.name());
    }
    println();
    print("ON ");
    tableVisit(x.getOn());

    if (x.isForEachRow()) {
      println();
      print("FOR EACH ROW");
    }
    decrementIndent();
    println();
    x.getBody().accept(this);
    return false;
  }

  /**
   * some table (or schema.table) , but not in SQLExprTableSource
   *
   * @param x
   * @return
   */
  private boolean tableVisit(SQLExpr x) {
    if (x instanceof SQLIdentifierExpr) {
      printTable(true, ((SQLIdentifierExpr) x).getName());
    } else if (x instanceof SQLPropertyExpr) {
      if (((SQLPropertyExpr) x).getOwner() instanceof SQLIdentifierExpr) {
        print(((SQLIdentifierExpr) ((SQLPropertyExpr) x).getOwner()).getName());
      } else {
        ((SQLPropertyExpr) x).getOwner().accept(this);
      }
      print(".");
      printTable(true, ((SQLPropertyExpr) x).getName());
    } else {
      x.accept(this);
    }
    return false;
  }

  private void printTable(boolean shouldReplace, String tableName) {
    boolean containBackQuote = tableName.indexOf(BACK_QOUTE) >= 0;
    if (containBackQuote) {
      tableName = tableName.replace(BACK_QOUTE, WHITESPACE_CHAR).trim();
    }
    if (shouldReplace && !tableAlias.contains(tableName)) {
      String replaceTableName = tableMap.get(ignoreCase ? tableName.toUpperCase() : tableName);
      tableName = replaceTableName != null ? replaceTableName : tableName;
    }
    if (containBackQuote) {
      print(BACK_QOUTE + tableName + BACK_QOUTE);
    } else {
      print(tableName);
    }
  }

  private boolean shouldReplace(SQLIdentifierExpr x) {
    if (tableMap == null || tableMap.isEmpty() || x.getParent() == null) {
      return false;
    }

    // simple table name
    if (x.getParent() instanceof SQLExprTableSource) {
      return true;
    }

    // show create table statement
    if (x.getParent() instanceof MySqlShowCreateTableStatement) {
      return true;
    }

    //table with property table.property
    if (x.getParent() instanceof SQLPropertyExpr
        && !(x.getParent().getParent() != null && x.getParent()
        .getParent() instanceof SQLExprTableSource)) {
      return true;
    }
    return false;
  }

  private boolean shouldReplace(SQLPropertyExpr x) {
    if (tableMap == null || tableMap.isEmpty() || x.getParent() == null) {
      return false;
    }

    // table with schema schema.table
    if (x.getParent() instanceof SQLExprTableSource) {
      return true;
    }

    // show create table statement
    if (x.getParent() instanceof MySqlShowCreateTableStatement) {
      return true;
    }

    return false;
  }

}
