/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.zhongan.dmds.commons.statistic.DataSourceSyncRecorder;
import com.zhongan.dmds.commons.statistic.HeartbeatRecorder;

/**
 * 2017.03 Extract interface IDBHeartbeat
 */
public interface IDBHeartbeat {

  public static final int DB_SYN_ERROR = -1;
  public static final int DB_SYN_NORMAL = 1;

  public static final int OK_STATUS = 1;
  public static final int ERROR_STATUS = -1;
  public static final int TIMEOUT_STATUS = -2;
  public static final int INIT_STATUS = 0;

  public Integer getSlaveBehindMaster();

  public int getDbSynStatus();

  public void setDbSynStatus(int dbSynStatus);

  public void setSlaveBehindMaster(Integer slaveBehindMaster);

  public int getStatus();

  public boolean isChecking();

  public void start();

  public void stop();

  public boolean isStop();

  public int getErrorCount();

  public HeartbeatRecorder getRecorder();

  public String getLastActiveTime();

  public long getTimeout();

  public void heartbeat();

  public long getHeartbeatTimeout();

  public void setHeartbeatTimeout(long heartbeatTimeout);

  public int getHeartbeatRetry();

  public void setHeartbeatRetry(int heartbeatRetry);

  public String getHeartbeatSQL();

  public void setHeartbeatSQL(String heartbeatSQL);

  public boolean isNeedHeartbeat();

  public DataSourceSyncRecorder getAsynRecorder();

}
