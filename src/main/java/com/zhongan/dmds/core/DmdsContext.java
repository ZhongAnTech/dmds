/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.zhongan.dmds.cache.CacheService;
import com.zhongan.dmds.commons.buffer.BufferPool;
import com.zhongan.dmds.commons.util.NameableExecutor;
import com.zhongan.dmds.commons.util.TimeUtil;
import com.zhongan.dmds.config.model.SystemConfig;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 上下文context 保存了全局的一些配置等公共对象 在服务器启动的时候手动 填充本对象
 * Move from (base on) MycatServer
 * 完善配置动态加载
 */
public class DmdsContext {

  private CountDownLatch initConnectionLatch;

  private static DmdsContext appCache = new DmdsContext();

  private static CacheService cacheService;
  private volatile SystemConfig system;

  private NameableExecutor businessExecutor;
  private NameableExecutor timerExecutor;
  private INIOProcessor[] processors;
  private final AtomicBoolean isOnline;

  private final long startupTime;

  private ListeningExecutorService listeningExecutorService;

  private volatile int nextProcessor;

  private SQLInterceptor sqlInterceptor;

  private BufferPool bufferPool;

  private Properties dnIndexProperties;

  private IDmdsConfig config;

  private SocketConnector connector;

  private final AtomicLong xaIDInc = new AtomicLong();

  private IRouteService routerService;

  public Properties getDnIndexProperties() {
    return dnIndexProperties;
  }

  public void setDnIndexProperties(Properties dnIndexProperties) {
    this.dnIndexProperties = dnIndexProperties;
  }

  public INIOProcessor[] getProcessors() {
    return processors;
  }

  public void setProcessors(INIOProcessor[] processors) {
    this.processors = processors;
  }

  private DmdsContext() {
    this.isOnline = new AtomicBoolean(true);
    this.startupTime = TimeUtil.currentTimeMillis();
  }

  public static DmdsContext getInstance() {
    return appCache;
  }

  public CacheService getCacheService() {
    return DmdsContext.cacheService;
  }

  public void setCacheService(CacheService cacheService) {
    DmdsContext.cacheService = cacheService;
  }

  public SystemConfig getSystem() {
    return system;
  }

  public void setSystem(SystemConfig system) {
    this.system = system;
  }

  public NameableExecutor getBusinessExecutor() {
    return businessExecutor;
  }

  public void setBusinessExecutor(NameableExecutor businessExecutor) {
    this.businessExecutor = businessExecutor;
  }

  public NameableExecutor getTimerExecutor() {
    return timerExecutor;
  }

  public void setTimerExecutor(NameableExecutor timerExecutor) {
    this.timerExecutor = timerExecutor;
  }

  public boolean isOnline() {
    return isOnline.get();
  }

  public void offline() {
    isOnline.set(false);
  }

  public void online() {
    isOnline.set(true);
  }

  public INIOProcessor nextProcessor() {
    int i = ++nextProcessor;
    if (i >= processors.length) {
      i = nextProcessor = 0;
    }
    return processors[i];
  }

  public ListeningExecutorService getListeningExecutorService() {
    return listeningExecutorService;
  }

  public void setListeningExecutorService(ListeningExecutorService listeningExecutorService) {
    this.listeningExecutorService = listeningExecutorService;
  }

  public long getStartupTime() {
    return startupTime;
  }

  public SQLInterceptor getSqlInterceptor() {
    return sqlInterceptor;
  }

  public void setSqlInterceptor(SQLInterceptor sqlInterceptor) {
    this.sqlInterceptor = sqlInterceptor;
  }

  public BufferPool getBufferPool() {
    return bufferPool;
  }

  public void setBufferPool(BufferPool bufferPool) {
    this.bufferPool = bufferPool;
  }

  public IDmdsConfig getConfig() {
    return config;
  }

  public void setConfig(IDmdsConfig config) {
    this.config = config;
  }

  public SocketConnector getConnector() {
    return connector;
  }

  public void setConnector(SocketConnector connector) {
    this.connector = connector;
  }

  public IRouteService getRouterservice() {
    return routerService;
  }

  public void setRouterService(IRouteService routerService) {
    this.routerService = routerService;
  }

  public CountDownLatch getInitConnectionLatch() {
    return initConnectionLatch;
  }

  public void setInitConnectionLatch(CountDownLatch initConnectionLatch) {
    this.initConnectionLatch = initConnectionLatch;
  }

  public String genXATXID() {
    long seq = this.xaIDInc.incrementAndGet();
    if (seq < 0) {
      synchronized (xaIDInc) {
        if (xaIDInc.get() < 0) {
          xaIDInc.set(0);
        }
        seq = xaIDInc.incrementAndGet();
      }
    }
    return "'Dmds." + this.getConfig().getSystem().getDmdsNodeId() + "." + seq + "'";
  }

}
