/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.sqlParser.druid.DmdsSchemaStatVisitor;

import java.sql.SQLNonTransientException;

public class DruidCreateTableParser extends DefaultDruidParser {

  @Override
  public void visitorParse(RouteResultset rrs, SQLStatement stmt, DmdsSchemaStatVisitor visitor) {
  }

  @Override
  public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt)
      throws SQLNonTransientException {
    MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement) stmt;
    if (createStmt.getQuery() != null) {
      String msg = "create table from other table not supported :" + stmt;
      LOGGER.warn(msg);
      throw new SQLNonTransientException(msg);
    }
    String tableName = StringUtil
        .removeBackquote(createStmt.getTableSource().toString().toUpperCase());
    ctx.addTable(tableName);

  }
}
