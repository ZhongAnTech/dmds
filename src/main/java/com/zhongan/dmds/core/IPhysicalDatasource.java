/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.zhongan.dmds.config.model.DBHostConfig;
import com.zhongan.dmds.config.model.DataHostConfig;

import java.io.IOException;

/**
 * Extract from PhysicalDatasource
 */
public interface IPhysicalDatasource {

  public boolean isMyConnection(BackendConnection con);

  public DataHostConfig getHostConfig();

  public boolean isReadNode();

  public int getSize();

  public IDBHeartbeat createHeartBeat();

  public IDBHeartbeat getHeartbeat();

  public String getName();

  public long getExecuteCount();

  public long getExecuteCountForSchema(String schema);

  public int getActiveCountForSchema(String schema);

  public int getIdleCountForSchema(String schema);

  public int getIdleCount();

  public int getIndex();

  public void heatBeatCheck(long timeout, long conHeartBeatPeriod);

  public int getActiveCount();

  public void clearCons(String reason);

  public void startHeartbeat();

  public void stopHeartbeat();

  public void doHeartbeat();

  public void getConnection(String schema, boolean autocommit, final ResponseHandler handler,
      final Object attachment) throws IOException;

  public void getConnection(String schema, boolean autocommit, final ResponseHandler handler,
      final Object attachment, boolean isInit) throws IOException;

  public DBHostConfig getConfig();

  public void setDbPool(IPhysicalDBPool dbPool);

  public void setHeartbeatRecoveryTime(long time);

  public long getHeartbeatRecoveryTime();
}
