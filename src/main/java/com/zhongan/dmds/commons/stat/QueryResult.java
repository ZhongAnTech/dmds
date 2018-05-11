/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.stat;

/**
 * SQL 执行结果
 *
 * @author zhuam
 */
public class QueryResult {

  private String user;
  private int sqlType;
  private String sql;
  private String ip;
  private long startTime;
  private long endTime;

  public QueryResult(String user, String ip, int sqlType, String sql, long startTime,
      long endTime) {
    super();
    this.user = user;
    this.sqlType = sqlType;
    this.sql = sql;
    this.ip = ip;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public String getUser() {
    return user;
  }

  public String getIp() {
    return ip;
  }

  public int getSqlType() {
    return sqlType;
  }

  public String getSql() {
    return sql;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }
}
