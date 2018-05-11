/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net;

import com.zhongan.dmds.commons.buffer.BufferPool;
import com.zhongan.dmds.commons.statistic.CommandCount;
import com.zhongan.dmds.commons.util.NameableExecutor;
import com.zhongan.dmds.commons.util.TimeUtil;
import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.INIOProcessor;
import com.zhongan.dmds.core.NIOConnection;
import com.zhongan.dmds.net.frontend.FrontendConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extract from NIOProcessor
 */
public final class NIOProcessor implements INIOProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger("NIOProcessor");
  private final String name;
  private final BufferPool bufferPool;
  private final NameableExecutor executor;
  private final ConcurrentMap<Long, NIOConnection> frontends;
  private final ConcurrentMap<Long, BackendConnection> backends;
  private final CommandCount commands;
  private long netInBytes;
  private long netOutBytes;

  // reload @@config_all 后, 老的backends  全部移往 backends_old, 待检测任务进行销毁
  public final static ConcurrentLinkedQueue<BackendConnection> backends_old = new ConcurrentLinkedQueue<BackendConnection>();

  // 前端已连接数
  private AtomicInteger frontendsLength = new AtomicInteger(0);

  public NIOProcessor(String name, BufferPool bufferPool, NameableExecutor executor)
      throws IOException {
    this.name = name;
    this.bufferPool = bufferPool;
    this.executor = executor;
    this.frontends = new ConcurrentHashMap<Long, NIOConnection>();
    this.backends = new ConcurrentHashMap<Long, BackendConnection>();
    this.commands = new CommandCount();
  }

  public String getName() {
    return name;
  }

  public BufferPool getBufferPool() {
    return bufferPool;
  }

  public int getWriteQueueSize() {
    int total = 0;
    for (NIOConnection fron : frontends.values()) {
      total += fron.getWriteQueue().size();
    }
    for (BackendConnection back : backends.values()) {
      if (back instanceof BackendNIOConnection) {
        total += ((BackendNIOConnection) back).getWriteQueue().size();
      }
    }
    return total;

  }

  public NameableExecutor getExecutor() {
    return this.executor;
  }

  public CommandCount getCommands() {
    return this.commands;
  }

  public long getNetInBytes() {
    return this.netInBytes;
  }

  public void addNetInBytes(long bytes) {
    this.netInBytes += bytes;
  }

  public long getNetOutBytes() {
    return this.netOutBytes;
  }

  public void addNetOutBytes(long bytes) {
    this.netOutBytes += bytes;
  }

  public void addFrontend(NIOConnection c) {
    this.frontends.put(c.getId(), (FrontendConnection) c);
    this.frontendsLength.incrementAndGet();
  }

  public ConcurrentMap<Long, NIOConnection> getFrontends() {
    return this.frontends;
  }

  public int getForntedsLength() {
    return this.frontendsLength.get();
  }

  public void addBackend(BackendConnection c) {
    this.backends.put(c.getId(), c);
  }

  public ConcurrentMap<Long, BackendConnection> getBackends() {
    return this.backends;
  }

  /**
   * 定时执行该方法，回收部分资源。
   */
  public void checkBackendCons() {
    backendCheck();
  }

  /**
   * 定时执行该方法，回收部分资源。
   */
  public void checkFrontCons() {
    frontendCheck();
  }

  // 前端连接检查
  private void frontendCheck() {
    Iterator<Entry<Long, NIOConnection>> it = frontends.entrySet().iterator();
    while (it.hasNext()) {
      NIOConnection c = it.next().getValue();
      // 删除空连接
      if (c == null) {
        it.remove();
        this.frontendsLength.decrementAndGet();
        continue;
      }
      // 清理已关闭连接，否则空闲检查。
      if (c.isClosed()) {
        //此处在高并发情况下会存在并发问题： backend execute error: java.nio.BufferOverflowException
        c.cleanup();
        it.remove();
        this.frontendsLength.decrementAndGet();
      } else {
        // very important ,for some data maybe not sent
        checkConSendQueue(c);
        c.idleCheck();
      }
    }
  }

  private void checkConSendQueue(NIOConnection c) {
    // very important ,for some data maybe not sent
    if (!c.getWriteQueue().isEmpty()) {
      c.getSocketWR().doNextWriteCheck();
    }
  }

  // 后端连接检查
  private void backendCheck() {
    long sqlTimeout = DmdsContext.getInstance().getSystem().getSqlExecuteTimeout() * 1000L;
    Iterator<Entry<Long, BackendConnection>> it = backends.entrySet().iterator();
    while (it.hasNext()) {
      BackendConnection c = it.next().getValue();
      // 删除空连接
      if (c == null) {
        it.remove();
        continue;
      }
      // SQL执行超时的连接关闭
      if (c.isBorrowed() && c.getLastTime() < TimeUtil.currentTimeMillis() - sqlTimeout) {
        LOGGER.warn("found backend connection SQL timeout ,close it " + c);
        c.close("sql timeout");
      }

      // 清理已关闭连接，否则空闲检查。
      if (c.isClosed()) {
        it.remove();
      } else {
        // very important ,for some data maybe not sent
        if (c instanceof AbstractConnection) {
          checkConSendQueue((AbstractConnection) c);
        }
        c.idleCheck();
      }
    }
  }

  public void removeConnection(NIOConnection con) {
    if (con instanceof BackendConnection) {
      this.backends.remove(con.getId());
    } else {
      this.frontends.remove(con.getId());
      this.frontendsLength.decrementAndGet();
    }
  }

  // jdbc连接用这个释放
  public void removeConnection(BackendConnection con) {
    this.backends.remove(con.getId());
  }
}