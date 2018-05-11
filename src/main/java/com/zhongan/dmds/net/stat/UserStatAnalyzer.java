/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.stat;

import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.commons.stat.QueryResult;
import com.zhongan.dmds.commons.stat.QueryResultListener;
import com.zhongan.dmds.core.DmdsContext;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 按访问用户 计算SQL的运行状态
 *
 * @author Ben
 */
public class UserStatAnalyzer implements QueryResultListener {

  private LinkedHashMap<String, UserStat> userStatMap = new LinkedHashMap<String, UserStat>();
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final static UserStatAnalyzer instance = new UserStatAnalyzer();

  private UserStatAnalyzer() {
  }

  public static UserStatAnalyzer getInstance() {
    return instance;
  }

  public void setSlowTime(long time) {
    // this.SQL_SLOW_TIME = time;
    DmdsContext.getInstance().getSystem().setSlowTime(time);
  }

  @Override
  public void onQueryResult(QueryResult query) {

    int sqlType = query.getSqlType();
    String sql = query.getSql();

    switch (sqlType) {
      case ServerParse.SELECT:
      case ServerParse.UPDATE:
      case ServerParse.INSERT:
      case ServerParse.DELETE:
      case ServerParse.REPLACE:

        String user = query.getUser();
        long startTime = query.getStartTime();
        long endTime = query.getEndTime();
        String ip = query.getIp();

        this.lock.writeLock().lock();
        try {
          UserStat userStat = userStatMap.get(user);
          if (userStat == null) {
            userStat = new UserStat(user);
            userStatMap.put(user, userStat);
          }
          userStat.update(sqlType, sql, ip, startTime, endTime);

        } finally {
          this.lock.writeLock().unlock();
        }
    }
  }

  public Map<String, UserStat> getUserStatMap() {
    Map<String, UserStat> map = new LinkedHashMap<String, UserStat>(userStatMap.size());
    this.lock.readLock().lock();
    try {
      map.putAll(userStatMap);
    } finally {
      this.lock.readLock().unlock();
    }
    return map;
  }
}
