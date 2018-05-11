/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.sqlParser.druid.impl.*;

/**
 * DruidParser的工厂类
 *
 * 2017.04 移除多其他数据库的支持，专注于MySQL
 */
public class DruidParserFactory {

  public static DruidParser create(SchemaConfig schema, SQLStatement statement,
      SchemaStatVisitor visitor) {
    DruidParser parser = null;
    if (statement instanceof SQLSelectStatement) {
      if (parser == null) {
        parser = new DruidSelectParser();
      }
    } else if (statement instanceof MySqlInsertStatement) {
      parser = new DruidInsertParser();
    } else if (statement instanceof MySqlDeleteStatement) {
      parser = new DruidDeleteParser();
    } else if (statement instanceof MySqlCreateTableStatement) {
      parser = new DruidCreateTableParser();
    } else if (statement instanceof MySqlUpdateStatement) {
      parser = new DruidUpdateParser();
    } else if (statement instanceof MySqlAlterTableStatement) {
      parser = new DruidAlterTableParser();
    } else {
      parser = new DefaultDruidParser();
    }
    return parser;
  }
}
