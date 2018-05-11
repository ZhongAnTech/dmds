/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.stat;

import com.zhongan.dmds.commons.stat.UserRWStat;
import com.zhongan.dmds.commons.stat.UserSqlHigh;
import com.zhongan.dmds.commons.stat.UserSqlStat;
import com.zhongan.dmds.commons.statistic.SQLRecord;
import com.zhongan.dmds.commons.statistic.SQLRecorder;
import com.zhongan.dmds.core.DmdsContext;

/**
 * 用户状态
 *
 * @author Ben
 */
public class UserStat {

  /**
   * SQL 执行记录
   */
  private UserSqlStat sqlStat = null;

  /**
   * CURD 执行分布
   */
  private UserRWStat rwStat = null;

  /**
   * 慢查询记录器 TOP 10
   */
  private SQLRecorder sqlRecorder;

  private String user;

  private UserSqlHigh sqlHighStat = null;

  public UserStat(String user) {
    super();
    this.user = user;
    this.rwStat = new UserRWStat();
    this.sqlStat = new UserSqlStat(50);
    this.sqlRecorder = new SQLRecorder(DmdsContext.getInstance().getSystem().getSqlRecordCount());
    this.sqlHighStat = new UserSqlHigh();
  }

  public String getUser() {
    return user;
  }

  public SQLRecorder getSqlRecorder() {
    return sqlRecorder;
  }

  public UserRWStat getRWStat() {
    return rwStat;
  }

  public UserSqlStat getSqlStat() {
    return sqlStat;
  }

  public void clearSql() {
    this.sqlStat.reset();
  }

  public void clearSqlslow() {
    this.sqlRecorder.clear();
  }

  public void reset() {
    this.sqlRecorder.clear();
    this.rwStat.reset();
    this.sqlStat.reset();
  }

  public void clearRwStat() {
    this.rwStat.reset();
  }

  /**
   * 更新状态
   *
   * @param sqlType
   * @param sql
   * @param startTime
   */
  public void update(int sqlType, String sql, String ip, long startTime, long endTime) {

    // 慢查询记录
    long executeTime = endTime - startTime;
    if (executeTime >= DmdsContext.getInstance().getSystem().getSlowTime()) { // SQL_SLOW_TIME
      SQLRecord record = new SQLRecord();
      record.executeTime = executeTime;
      record.statement = sql;
      record.startTime = startTime;
      record.host = ip;
      this.sqlRecorder.add(record);
    }

    // 执行状态记录
    this.rwStat.add(sqlType, executeTime, startTime, endTime);

    // 记录SQL
    this.sqlStat.add(sql, ip, executeTime, startTime, endTime);

    // 记录高频SQL
    this.sqlHighStat.addSql(sql, executeTime, startTime, endTime);
  }

  public UserSqlHigh getSqlHigh() {
    return this.sqlHighStat;
  }
}
