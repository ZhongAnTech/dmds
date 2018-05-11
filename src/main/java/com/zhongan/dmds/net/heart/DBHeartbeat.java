/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.heart;

import com.zhongan.dmds.commons.statistic.DataSourceSyncRecorder;
import com.zhongan.dmds.commons.statistic.HeartbeatRecorder;
import com.zhongan.dmds.core.IDBHeartbeat;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 2017.03 Extract interface IDBHeartbeat
 */
public abstract class DBHeartbeat implements IDBHeartbeat {

  private static final long DEFAULT_HEARTBEAT_TIMEOUT = 30 * 1000L;
  private static final int DEFAULT_HEARTBEAT_RETRY = 10;
  // heartbeat config
  public long heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT; // 心跳超时时间
  public int heartbeatRetry = DEFAULT_HEARTBEAT_RETRY; // 检查连接发生异常到切换，重试次数

  public String heartbeatSQL;// 静态心跳语句
  public final AtomicBoolean isStop = new AtomicBoolean(true);
  public final AtomicBoolean isChecking = new AtomicBoolean(false);
  public int errorCount;
  public volatile int status;
  public final HeartbeatRecorder recorder = new HeartbeatRecorder();
  public final DataSourceSyncRecorder asynRecorder = new DataSourceSyncRecorder();

  private volatile Integer slaveBehindMaster;
  private volatile int dbSynStatus = DB_SYN_NORMAL;

  public Integer getSlaveBehindMaster() {
    return slaveBehindMaster;
  }

  public int getDbSynStatus() {
    return dbSynStatus;
  }

  public void setDbSynStatus(int dbSynStatus) {
    this.dbSynStatus = dbSynStatus;
  }

  public void setSlaveBehindMaster(Integer slaveBehindMaster) {
    this.slaveBehindMaster = slaveBehindMaster;
  }

  public int getStatus() {
    return status;
  }

  public boolean isChecking() {
    return isChecking.get();
  }

  public abstract void start();

  public abstract void stop();

  public boolean isStop() {
    return isStop.get();
  }

  public int getErrorCount() {
    return errorCount;
  }

  public HeartbeatRecorder getRecorder() {
    return recorder;
  }

  public abstract String getLastActiveTime();

  public abstract long getTimeout();

  public abstract void heartbeat();

  public long getHeartbeatTimeout() {
    return heartbeatTimeout;
  }

  public void setHeartbeatTimeout(long heartbeatTimeout) {
    this.heartbeatTimeout = heartbeatTimeout;
  }

  public int getHeartbeatRetry() {
    return heartbeatRetry;
  }

  public void setHeartbeatRetry(int heartbeatRetry) {
    this.heartbeatRetry = heartbeatRetry;
  }

  public String getHeartbeatSQL() {
    return heartbeatSQL;
  }

  public void setHeartbeatSQL(String heartbeatSQL) {
    this.heartbeatSQL = heartbeatSQL;
  }

  public boolean isNeedHeartbeat() {
    return heartbeatSQL != null;
  }

  public DataSourceSyncRecorder getAsynRecorder() {
    return this.asynRecorder;
  }

}