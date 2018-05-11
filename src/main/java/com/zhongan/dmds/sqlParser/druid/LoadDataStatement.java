/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;

public class LoadDataStatement extends MySqlLoadDataInFileStatement {

  public String toString() {
    StringBuilder out = new StringBuilder();
    this.accept(new LoadDataOutputVisitor(out));

    return out.toString();
  }
}
