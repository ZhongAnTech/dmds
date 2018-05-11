/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

/**
 * used for interceptor sql before execute ,can modify sql befor execute
 */
public interface SQLInterceptor {

  /**
   * return new sql to handler,ca't modify sql's type
   *
   * @param sql
   * @param sqlType
   * @return new sql
   */
  String interceptSQL(String sql, int sqlType, IServerConnection sc);
}
