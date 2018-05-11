/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.interceptor.impl;

import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.core.SQLInterceptor;

public class StatSqlInterceptor implements SQLInterceptor {

  @Override
  public String interceptSQL(String sql, int sqlType, IServerConnection sc) {
    final int atype = sqlType;
    final String sqls = DefaultSqlInterceptor.processEscape(sql);
    return sql;
  }

}
