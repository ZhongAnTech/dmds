/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.zhongan.dmds.cache.CacheService;
import com.zhongan.dmds.commons.buffer.BufferPool;
import com.zhongan.dmds.commons.statistic.SQLRecorder;
import com.zhongan.dmds.commons.util.ExecutorUtil;
import com.zhongan.dmds.commons.util.NameableExecutor;
import com.zhongan.dmds.commons.util.TimeUtil;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.core.*;
import com.zhongan.dmds.manager.ManagerConnectionFactory;
import com.zhongan.dmds.net.NIOAcceptor;
import com.zhongan.dmds.net.NIOConnector;
import com.zhongan.dmds.net.NIOProcessor;
import com.zhongan.dmds.net.NIOReactorPool;
import com.zhongan.dmds.route.RouteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Base on MycatServer
 * remove support for AIO
 * remove support for isOnline
 * move some fields to dmdsContext
 */
public class DmdsServer {

  public static final String NAME = "DMDS";
  private static final long TIME_UPDATE_PERIOD = 20L;
  private static final DmdsServer INSTANCE = new DmdsServer();
  private static final Logger LOGGER = LoggerFactory.getLogger(DmdsServer.class);
  ;

  private AsynchronousChannelGroup[] asyncChannelGroups;
  private volatile int channelIndex = 0;

  public static final DmdsServer getInstance() {
    return INSTANCE;
  }

  private final Timer timer;
  private final SQLRecorder sqlRecorder;

  public DmdsServer() {
    // 载入配置文件
    IDmdsConfig config = new DmdsConfig();
    DmdsContext.getInstance().setConfig(config);
    DmdsContext.getInstance().setSystem(config.getSystem());
    this.timer = new Timer(NAME + "Timer", true);
    this.sqlRecorder = new SQLRecorder(config.getSystem().getSqlRecordCount());

    CacheService cacheService = new CacheService();
    DmdsContext.getInstance().setCacheService(cacheService);

    IRouteService routerService = new RouteService(cacheService);
    DmdsContext.getInstance().setRouterService(routerService);

    // load datanode active index from properties
    Properties dnIndexProperties = loadDnIndexProps();
    DmdsContext.getInstance().setDnIndexProperties(dnIndexProperties);

    try {
      SQLInterceptor sqlInterceptor = (SQLInterceptor) Class
          .forName(config.getSystem().getSqlInterceptor()).newInstance();
      DmdsContext.getInstance().setSqlInterceptor(sqlInterceptor);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * get next AsynchronousChannel ,first is exclude if multi AsynchronousChannelGroups
   *
   * @return
   */
  public AsynchronousChannelGroup getNextAsyncChannelGroup() {
    if (asyncChannelGroups.length == 1) {
      return asyncChannelGroups[0];
    } else {
      int index = (++channelIndex) % asyncChannelGroups.length;
      if (index == 0) {
        ++channelIndex;
        return asyncChannelGroups[1];
      } else {
        return asyncChannelGroups[index];
      }
    }
  }

  public void startup() throws IOException {
    SystemConfig system = DmdsContext.getInstance().getSystem();
    int processorCount = system.getProcessors();

    // server startup
    LOGGER.info(
        "===============================================================================================");
    LOGGER.info(NAME + " is ready to startup ...");

    StringBuilder inf = new StringBuilder("Startup processors ...,total processors:")
        .append(system.getProcessors() + ",aio thread pool size:")
        .append(system.getProcessorExecutor())
        .append("    \\r\\n each process allocated socket buffer pool ")
        .append(" bytes ,buffer chunk size:")
        .append(system.getProcessorBufferChunk())
        .append("  buffer pool's capacity(buferPool/bufferChunk) is:")
        .append(system.getProcessorBufferPool() / system.getProcessorBufferChunk());

    LOGGER.info(inf.toString());
    LOGGER.info("sysconfig params:" + system.toString());

    // startup manager
    ManagerConnectionFactory mf = new ManagerConnectionFactory();
    ServerConnectionFactory sf = new ServerConnectionFactory();
    SocketAcceptor manager = null;
    SocketAcceptor server = null;

    // startup processors
    int threadPoolSize = system.getProcessorExecutor();

    long processBuferPool = system.getProcessorBufferPool();
    int processBufferChunk = system.getProcessorBufferChunk();
    int socketBufferLocalPercent = system.getProcessorBufferLocalPercent();

    BufferPool bufferPool = new BufferPool(processBuferPool, processBufferChunk,
        socketBufferLocalPercent / processorCount);
    DmdsContext.getInstance().setBufferPool(bufferPool);

    NameableExecutor businessExecutor = ExecutorUtil.create("BusinessExecutor", threadPoolSize);
    DmdsContext.getInstance().setBusinessExecutor(businessExecutor);

    NameableExecutor timerExecutor = ExecutorUtil.create("Timer", system.getTimerExecutor());
    DmdsContext.getInstance().setTimerExecutor(timerExecutor);

    ListeningExecutorService listeningExecutorService = MoreExecutors
        .listeningDecorator(businessExecutor);
    DmdsContext.getInstance().setListeningExecutorService(listeningExecutorService);

    INIOProcessor[] processors = new NIOProcessor[processorCount];
    for (int i = 0; i < processors.length; i++) {
      processors[i] = new NIOProcessor("Processor" + i, bufferPool, businessExecutor);
    }
    DmdsContext.getInstance().setProcessors(processors);

    LOGGER.info("using nio network handler ");
    NIOReactorPool reactorPool = new NIOReactorPool(BufferPool.LOCAL_BUF_THREAD_PREX + "NIOREACTOR",
        processors.length);
    SocketConnector connector = new NIOConnector(BufferPool.LOCAL_BUF_THREAD_PREX + "NIOConnector",
        reactorPool);
    ((NIOConnector) connector).start();
    DmdsContext.getInstance().setConnector(connector);

    manager = new NIOAcceptor(BufferPool.LOCAL_BUF_THREAD_PREX + NAME + "Manager",
        system.getBindIp(), system.getManagerPort(), mf, reactorPool);
    server = new NIOAcceptor(BufferPool.LOCAL_BUF_THREAD_PREX + NAME + "Server", system.getBindIp(),
        system.getServerPort(), sf, reactorPool);

    LOGGER.info(manager.getName() + " is started and listening on " + manager.getPort());
    LOGGER.info(server.getName() + " is started and listening on " + server.getPort());
    LOGGER.info(
        "===============================================================================================");

    // init datahost
    Map<String, IPhysicalDBPool> dataHosts = DmdsContext.getInstance().getConfig().getDataHosts();
    LOGGER.info("Initialize dataHost ...");

    int initConnectionSize = 0;
    for (IPhysicalDBPool node : dataHosts.values()) {
      String index = DmdsContext.getInstance().getDnIndexProperties()
          .getProperty(node.getHostName(), "0");
      if (!"0".equals(index)) {
        LOGGER.info("init datahost:{}  to use datasource index:{} ", node.getHostName(), index);
      }

      IPhysicalDatasource[] writeSources = node.getSources();

      for (IPhysicalDatasource physicalDatasource : writeSources) {
        initConnectionSize += physicalDatasource.getHostConfig().getMinCon();
      }
    }
    LOGGER.info("initConnectionSize={}", initConnectionSize);
    CountDownLatch initConnectionLatch = new CountDownLatch(initConnectionSize);
    DmdsContext.getInstance().setInitConnectionLatch(initConnectionLatch);

    for (IPhysicalDBPool node : dataHosts.values()) {
      String index = DmdsContext.getInstance().getDnIndexProperties()
          .getProperty(node.getHostName(), "0");
      if (!"0".equals(index)) {
        LOGGER.info("init datahost:{}  to use datasource index:{} ", node.getHostName(), index);
      }
      node.init(Integer.valueOf(index));
      node.startHeartbeat();
    }

    try {
      initConnectionLatch.await(60000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.info("初始化数据源await异常：", e);
    }

    LOGGER.info("start  server");
    manager.start();
    server.start();

    long dataNodeIdleCheckPeriod = system.getDataNodeIdleCheckPeriod();
    //系统时间定时更新任务
    timer.schedule(updateTime(), 0L, TIME_UPDATE_PERIOD);

    //检查前、后端连接将空连接和已经关闭的连接从队列中剔除，并且会对在socket中没有发送的数据发送以及idle线程检查
    timer.schedule(processorCheck(), 0L, system.getProcessorCheckPeriod());

    //检查数据节点定时连接空闲超时检查任务如果多余min conn则close， 如果少于min conn则create
    timer.schedule(dataNodeConHeartBeatCheck(dataNodeIdleCheckPeriod), 0L, dataNodeIdleCheckPeriod);

    //数据节点定时心跳
    timer.schedule(dataNodeHeartbeat(), 0L, system.getDataNodeHeartbeatPeriod());
  }

  private Properties loadDnIndexProps() {
    Properties prop = new Properties();
    File file = new File(SystemConfig.getHomePath(),
        "conf" + File.separator + "conf/dnindex.properties");
    if (!file.exists()) {
      return prop;
    }
    FileInputStream filein = null;
    try {
      filein = new FileInputStream(file);
      prop.load(filein);
    } catch (Exception e) {
      LOGGER.warn("load DataNodeIndex err:" + e);
    } finally {
      if (filein != null) {
        try {
          filein.close();
        } catch (IOException e) {
        }
      }
    }
    return prop;
  }

  /**
   * save cur datanode index to properties file
   *
   * @param dataNode
   * @param curIndex
   */
  public synchronized void saveDataHostIndex(String dataHost, int curIndex) {
    File file = new File(SystemConfig.getHomePath(),
        "conf" + File.separator + "conf/dnindex.properties");
    FileOutputStream fileOut = null;
    try {
      String oldIndex = DmdsContext.getInstance().getDnIndexProperties().getProperty(dataHost);
      String newIndex = String.valueOf(curIndex);
      if (newIndex.equals(oldIndex)) {
        return;
      }
      DmdsContext.getInstance().getDnIndexProperties().setProperty(dataHost, newIndex);
      LOGGER.info("save DataHost index  " + dataHost + " cur index " + curIndex);

      File parent = file.getParentFile();
      if (parent != null && !parent.exists()) {
        parent.mkdirs();
      }
      fileOut = new FileOutputStream(file);
      DmdsContext.getInstance().getDnIndexProperties().store(fileOut, "update");
    } catch (Exception e) {
      LOGGER.warn("saveDataNodeIndex err:", e);
    } finally {
      if (fileOut != null) {
        try {
          fileOut.close();
        } catch (IOException e) {
        }
      }
    }
  }

  public SQLRecorder getSqlRecorder() {
    return sqlRecorder;
  }

  // 系统时间定时更新任务
  private TimerTask updateTime() {
    return new TimerTask() {
      @Override
      public void run() {
        TimeUtil.update();
      }
    };
  }

  // 处理器定时检查任务
  private TimerTask processorCheck() {
    return new TimerTask() {
      @Override
      public void run() {
        DmdsContext.getInstance().getTimerExecutor().execute(new Runnable() {
          @Override
          public void run() {
            try {
              //检查后端连接
              for (INIOProcessor p : DmdsContext.getInstance().getProcessors()) {
                p.checkBackendCons();
              }
            } catch (Exception e) {
              LOGGER.warn("checkBackendCons caught err:" + e);
            }
          }
        });
        DmdsContext.getInstance().getTimerExecutor().execute(new Runnable() {
          @Override
          public void run() {
            try {
              //检查前端连接
              for (INIOProcessor p : DmdsContext.getInstance().getProcessors()) {
                p.checkFrontCons();
              }
            } catch (Exception e) {
              LOGGER.warn("checkFrontCons caught err:" + e);
            }
          }
        });
      }
    };
  }

  // 数据节点定时连接空闲超时检查任务
  private TimerTask dataNodeConHeartBeatCheck(final long heartPeriod) {
    return new TimerTask() {
      @Override
      public void run() {
        DmdsContext.getInstance().getTimerExecutor().execute(new Runnable() {
          @Override
          public void run() {
            Map<String, IPhysicalDBPool> nodes = DmdsContext.getInstance().getConfig()
                .getDataHosts();
            for (IPhysicalDBPool node : nodes.values()) {
              node.heartbeatCheck(heartPeriod);
            }
            Map<String, IPhysicalDBPool> _nodes = DmdsContext.getInstance().getConfig()
                .getBackupDataHosts();
            if (_nodes != null) {
              for (IPhysicalDBPool node : _nodes.values()) {
                node.heartbeatCheck(heartPeriod);
              }
            }
          }
        });
      }
    };
  }

  // 数据节点定时心跳任务
  private TimerTask dataNodeHeartbeat() {
    return new TimerTask() {
      @Override
      public void run() {
        DmdsContext.getInstance().getTimerExecutor().execute(new Runnable() {
          @Override
          public void run() {
            Map<String, IPhysicalDBPool> nodes = DmdsContext.getInstance().getConfig()
                .getDataHosts();
            for (IPhysicalDBPool node : nodes.values()) {
              node.doHeartbeat();
            }
          }
        });
      }
    };
  }

}