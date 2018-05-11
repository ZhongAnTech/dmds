/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.jdbc;

import com.zhongan.dmds.commons.statistic.HeartbeatRecorder;
import com.zhongan.dmds.core.IDBHeartbeat;
import com.zhongan.dmds.net.heart.DBHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

public class JDBCHeartbeat extends DBHeartbeat {

  private final ReentrantLock lock;
  private final JDBCDatasource source;
  private final boolean heartbeatnull;
  private Long lastSendTime = System.currentTimeMillis();
  private Long lastReciveTime = System.currentTimeMillis();

  private Logger logger = LoggerFactory.getLogger(this.getClass());

  public JDBCHeartbeat(JDBCDatasource source) {
    this.source = source;
    lock = new ReentrantLock(false);
    this.status = IDBHeartbeat.INIT_STATUS;
    this.heartbeatSQL = source.getHostConfig().getHearbeatSQL().trim();
    this.heartbeatnull = heartbeatSQL.length() == 0;
  }

  @Override
  public void start() {
    if (this.heartbeatnull) {
      stop();
      return;
    }
    lock.lock();
    try {
      isStop.compareAndSet(true, false);
      this.status = DBHeartbeat.OK_STATUS;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void stop() {
    lock.lock();
    try {
      if (isStop.compareAndSet(false, true)) {
        isChecking.set(false);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String getLastActiveTime() {
    long t = lastReciveTime;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    return sdf.format(new Date(t));
  }

  @Override
  public long getTimeout() {
    return 0;
  }

  @Override
  public HeartbeatRecorder getRecorder() {
    recorder.set(lastReciveTime - lastSendTime);
    return recorder;
  }

  @Override
  public void heartbeat() {

    if (isStop.get()) {
      return;
    }
    lastSendTime = System.currentTimeMillis();
    lock.lock();
    try {
      isChecking.set(true);

      try (Connection c = source.getConnection()) {
        try (Statement s = c.createStatement()) {
          s.execute(heartbeatSQL);
        }
      }
      status = IDBHeartbeat.OK_STATUS;
      if (logger.isDebugEnabled()) {
        logger.debug("JDBCHeartBeat con query sql: " + heartbeatSQL);
      }

    } catch (Exception ex) {
      logger.error("JDBCHeartBeat error", ex);
      status = IDBHeartbeat.ERROR_STATUS;
    } finally {
      lock.unlock();
      this.isChecking.set(false);
      lastReciveTime = System.currentTimeMillis();
    }
  }
}
