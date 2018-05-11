/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.interceptor.impl;

import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.core.SQLInterceptor;

/**
 * Fix problem when use 'IF NOT EXISTS'
 */
public class DefaultSqlInterceptor implements SQLInterceptor {

  private static final char ESCAPE_CHAR = '\\';

  private static final int TARGET_STRING_LENGTH = 2;

  /**
   * mysql driver对'转义与\',解析前改为foundationdb parser支持的''
   *
   * @param stmt replace regex with general string walking avoid sql being destroyed in case of some
   *             mismatch maybe some performance enchanced
   * @return
   */
  public static String processEscape(String sql) {
    int firstIndex = -1;
    if ((sql == null) || ((firstIndex = sql.indexOf(ESCAPE_CHAR)) == -1)) {
      return sql;
    } else {
      int lastIndex = sql.lastIndexOf(ESCAPE_CHAR, sql.length() - 2) + TARGET_STRING_LENGTH;
      StringBuilder sb = new StringBuilder(sql);
      for (int i = firstIndex; i < lastIndex; i++) {
        if (sb.charAt(i) == '\\') {
          if (i + 1 < lastIndex) {
            if (sb.charAt(i + 1) == '\'') {
              // replace
              sb.setCharAt(i, '\'');
            }
          }
          // roll over
          i++;
        }
      }
      return sb.toString();
    }
  }

  /**
   * 替换ddl语句中的 IF NOT EXISTS
   *
   * @param stmt
   * @return
   */
  public static String replaceDDLIfNotExists(String sql, int sqlType) {
    if (ServerParse.DDL == sqlType) {
      if (sql.toUpperCase().contains("IF NOT EXISTS ")) {
        sql = sql.toUpperCase().replace("IF NOT EXISTS ", "");
        return sql;
      }
    }
    return sql;
  }

  /**
   * escape mysql escape letter sql type ServerParse.UPDATE,ServerParse.INSERT etc TODO fixed it
   */
  @Override
  public String interceptSQL(String sql, int sqlType, IServerConnection sc) {

    if ("fdbparser"
        .equals(DmdsContext.getInstance().getConfig().getSystem().getDefaultSqlParser())) {
      sql = processEscape(sql);
    }

    sql = replaceDDLIfNotExists(sql, sqlType);

    return sql;
  }
}
