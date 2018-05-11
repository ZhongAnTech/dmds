/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.route.RouteResultset;

import java.sql.SQLNonTransientException;

public class DruidDeleteParser extends DefaultDruidParser {

  @Override
  public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt)
      throws SQLNonTransientException {
    MySqlDeleteStatement delete = (MySqlDeleteStatement) stmt;
    String tableName = StringUtil
        .removeBackquote(delete.getTableName().getSimpleName().toUpperCase());
    ctx.addTable(tableName);
  }
}
