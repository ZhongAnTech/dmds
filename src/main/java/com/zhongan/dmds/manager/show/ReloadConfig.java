/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.config.model.DmdsCluster;
import com.zhongan.dmds.config.model.QuarantineConfig;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.UserConfig;
import com.zhongan.dmds.config.util.DnPropertyUtil;
import com.zhongan.dmds.core.*;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.backend.ConfigInitializer;
import com.zhongan.dmds.net.protocol.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 2019.01 完善配置热加载
 */
public final class ReloadConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReloadConfig.class);

  public static void execute(ManagerConnection c, final boolean loadAll) {
    final ReentrantLock lock = DmdsContext.getInstance().getConfig().getLock();
    lock.lock();
    try {
      ListenableFuture<Boolean> listenableFuture = DmdsContext.getInstance()
          .getListeningExecutorService()
          .submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {

              return loadAll ? reload_all() : reload();
            }
          });
      Futures.addCallback(listenableFuture, new ReloadCallBack(c),
          DmdsContext.getInstance().getListeningExecutorService());
    } finally {
      lock.unlock();
    }
  }

  private static boolean reload_all() {
    // 载入新的配置
    ConfigInitializer loader = new ConfigInitializer(true);
    Map<String, UserConfig> users = loader.getUsers();
    Map<String, SchemaConfig> schemas = loader.getSchemas();
    Map<String, IPhysicalDBNode> dataNodes = loader.getDataNodes();
    Map<String, IPhysicalDBPool> dataHosts = loader.getDataHosts();
    DmdsCluster cluster = loader.getCluster();
    QuarantineConfig quarantine = loader.getQuarantine();

    // 应用新配置
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    conf.setDataNodes(dataNodes);

    //统计需要初始化连接的个数
    int initConnectionSize = 0;
    for (IPhysicalDBPool node : dataHosts.values()) {
      String index = DmdsContext.getInstance().getDnIndexProperties()
          .getProperty(node.getHostName(), "0");
      if (!"0".equals(index)) {
        LOGGER.info("init datahost: " + node.getHostName() + "  to use datasource index:" + index);
      }

      IPhysicalDatasource[] writeSources = node.getSources();

      for (IPhysicalDatasource physicalDatasource : writeSources) {
        initConnectionSize += physicalDatasource.getHostConfig().getMinCon();
      }
    }
    LOGGER.info("reoad initConnectionSize=" + initConnectionSize);
    CountDownLatch initConnectionLatch = new CountDownLatch(initConnectionSize);
    DmdsContext.getInstance().setInitConnectionLatch(initConnectionLatch);

    Map<String, IPhysicalDBPool> cNodes = conf.getDataHosts();
    boolean reloadStatus = true;
    for (IPhysicalDBPool dn : dataHosts.values()) {
      String[] sh = DmdsContext.getInstance().getConfig()
          .getDataNodeSchemasOfDataHost(dn.getHostName());
      LOGGER.info("set Schemas: " + sh[0] + ",hostName:" + dn.getHostName());
      dn.setSchemas(
          DmdsContext.getInstance().getConfig().getDataNodeSchemasOfDataHost(dn.getHostName()));
      // init datahost
      String index = DnPropertyUtil.loadDnIndexProps().getProperty(dn.getHostName(), "0");
      if (!"0".equals(index)) {
        LOGGER.info("init datahost: " + dn.getHostName() + "  to use datasource index:" + index);
      }
      dn.init(Integer.valueOf(index));

      // dn.init(0);
      if (!dn.isInitSuccess()) {
        reloadStatus = false;
        break;
      }

    }

    //等待数据源初始化成功
    try {
      DmdsContext.getInstance().getInitConnectionLatch().await(60000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.info("热加载初始化数据源await异常：" + e);
      reloadStatus = false;
    }

    LOGGER.info("热加载 新数据源初始化完成");

    // 如果重载不成功，则清理已初始化的资源。
    if (!reloadStatus) {
      LOGGER.warn("reload failed ,clear previously created datasources ");
      for (IPhysicalDBPool dn : dataHosts.values()) {
        dn.clearDataSources("reload config");
        dn.stopHeartbeat();
      }
      return false;
    }

    // 应用重载
    conf.reload(users, schemas, dataNodes, dataHosts, cluster, quarantine, true);

    // 处理旧的资源
    for (IPhysicalDBPool dn : cNodes.values()) {
      dn.clearDataSources("reload config clear old datasources");
      dn.stopHeartbeat();
    }

    // 清理缓存
    DmdsContext.getInstance().getCacheService().clearCache();
    return true;
  }

  private static boolean reload() {
    // 载入新的配置
    ConfigInitializer loader = new ConfigInitializer(false);
    Map<String, UserConfig> users = loader.getUsers();
    Map<String, SchemaConfig> schemas = loader.getSchemas();
    Map<String, IPhysicalDBNode> dataNodes = loader.getDataNodes();
    Map<String, IPhysicalDBPool> dataHosts = loader.getDataHosts();
    DmdsCluster cluster = loader.getCluster();
    QuarantineConfig quarantine = loader.getQuarantine();

    // 应用新配置
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();

    // 应用重载
    conf.reload(users, schemas, dataNodes, dataHosts, cluster, quarantine, false);

    // 清理缓存
    DmdsContext.getInstance().getCacheService().clearCache();
    return true;
  }

  /**
   * 异步执行回调类，用于回写数据给用户等。
   */
  private static class ReloadCallBack implements FutureCallback<Boolean> {

    private ManagerConnection mc;

    private ReloadCallBack(ManagerConnection c) {
      this.mc = c;
    }

    @Override
    public void onSuccess(Boolean result) {
      if (result) {
        LOGGER.warn("send ok package to client " + String.valueOf(mc));
        OkPacket ok = new OkPacket();
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
        ok.message = "Reload config success".getBytes();
        ok.write(mc);
      } else {
        mc.writeErrMessage(ErrorCode.ER_YES, "Reload config failure");
      }
    }

    @Override
    public void onFailure(Throwable t) {
      mc.writeErrMessage(ErrorCode.ER_YES, "Reload config failure");
    }
  }
}