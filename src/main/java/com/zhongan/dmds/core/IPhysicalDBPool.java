/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Extract from PhysicalDBPool
 */
public interface IPhysicalDBPool {

  public abstract int getWriteType();

  public abstract IPhysicalDatasource findDatasouce(BackendConnection exitsCon);

  public abstract String getHostName();

  /**
   * all write datanodes
   *
   * @return
   */
  public abstract IPhysicalDatasource[] getSources();

  public abstract IPhysicalDatasource getSource();

  public abstract int getActivedIndex();

  public abstract boolean isInitSuccess();

  public abstract int next(int i);

  public abstract boolean switchSource(int newIndex, boolean isAlarm,
      String reason);

  public abstract void init(int index);

  public abstract void doHeartbeat();

  /**
   * back physical connection heartbeat check
   */
  public abstract void heartbeatCheck(long ildCheckPeriod);

  public abstract void startHeartbeat();

  public abstract void stopHeartbeat();

  public abstract void clearDataSources(String reason);

  public abstract Collection<IPhysicalDatasource> genAllDataSources();

  public abstract Collection<IPhysicalDatasource> getAllDataSources();

  /**
   * return connection for read balance
   *
   * @param handler
   * @param attachment
   * @param database
   * @throws Exception
   */
  public abstract void getRWBanlanceCon(String schema, boolean autocommit,
      ResponseHandler handler, Object attachment, String database)
      throws Exception;

  /**
   * 随机选择，按权重设置随机概率。 在一个截面上碰撞的概率高，但调用量越大分布越均匀，而且按概率使用权重后也比较均匀，有利于动态调整提供者权重。
   *
   * @param okSources
   * @return
   */
  public abstract IPhysicalDatasource randomSelect(ArrayList<IPhysicalDatasource> okSources);

  public abstract int getBalance();

  public abstract String[] getSchemas();

  public abstract void setSchemas(String[] mySchemas);
}
