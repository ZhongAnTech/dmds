/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableStatement;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.sqlParser.druid.DmdsSchemaStatVisitor;

import java.sql.SQLNonTransientException;

/**
 * alter table 语句解析
 */
public class DruidAlterTableParser extends DefaultDruidParser {

  @Override
  public void visitorParse(RouteResultset rrs, SQLStatement stmt, DmdsSchemaStatVisitor visitor)
      throws SQLNonTransientException {

  }

  @Override
  public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt)
      throws SQLNonTransientException {
    MySqlAlterTableStatement alterTable = (MySqlAlterTableStatement) stmt;
    String tableName = StringUtil
        .removeBackquote(alterTable.getTableSource().toString().toUpperCase());

    ctx.addTable(tableName);

  }
}
