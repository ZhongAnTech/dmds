/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.stat;

import com.zhongan.dmds.commons.parse.ServerParse;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 高频SQL
 */
public class HighFrequencySqlAnalyzer implements QueryResultListener {

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final static HighFrequencySqlAnalyzer instance = new HighFrequencySqlAnalyzer();

  private HighFrequencySqlAnalyzer() {
  }

  public static HighFrequencySqlAnalyzer getInstance() {
    return instance;
  }

  @Override
  public void onQueryResult(QueryResult queryResult) {
    int sqlType = queryResult.getSqlType();
    String sql = queryResult.getSql();
    // String newSql = this.sqlParser.mergeSql(sql);
    long executeTime = queryResult.getEndTime() - queryResult.getStartTime();
    this.lock.writeLock().lock();
    try {
      switch (sqlType) {
        case ServerParse.SELECT:
        case ServerParse.UPDATE:
        case ServerParse.INSERT:
        case ServerParse.DELETE:
        case ServerParse.REPLACE:
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

}