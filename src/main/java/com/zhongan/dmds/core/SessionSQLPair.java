/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.zhongan.dmds.config.model.SchemaConfig;

public class SessionSQLPair {

  public final Session session;

  public final SchemaConfig schema;
  public final String sql;
  public final int type;

  public SessionSQLPair(Session session, SchemaConfig schema, String sql, int type) {
    super();
    this.session = session;
    this.schema = schema;
    this.sql = sql;
    this.type = type;
  }

}
