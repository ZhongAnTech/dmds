/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.zhongan.dmds.commons.buffer.BufferPool;
import com.zhongan.dmds.commons.statistic.CommandCount;
import com.zhongan.dmds.commons.util.NameableExecutor;

import java.util.concurrent.ConcurrentMap;

/**
 * Extract from NIOProcessor
 */
public interface INIOProcessor {

  public String getName();

  public BufferPool getBufferPool();

  public int getWriteQueueSize();

  public NameableExecutor getExecutor();

  public CommandCount getCommands();

  public long getNetInBytes();

  public void addNetInBytes(long bytes);

  public long getNetOutBytes();

  public void addNetOutBytes(long bytes);

  public void checkBackendCons();

  /**
   * 定时执行该方法，回收部分资源。
   */
  public void checkFrontCons();

  public void removeConnection(NIOConnection con);

  public void addBackend(BackendConnection c);

  public void addFrontend(NIOConnection c);

  public ConcurrentMap<Long, NIOConnection> getFrontends();

  public ConcurrentMap<Long, BackendConnection> getBackends();

  public int getForntedsLength();

}
