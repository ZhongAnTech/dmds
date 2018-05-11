/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.backend;

import com.zhongan.dmds.commons.util.TimeUtil;
import com.zhongan.dmds.config.Alarms;
import com.zhongan.dmds.config.model.DBHostConfig;
import com.zhongan.dmds.config.model.DataHostConfig;
import com.zhongan.dmds.core.*;
import com.zhongan.dmds.net.mysql.nio.MySQLConnection;
import com.zhongan.dmds.net.mysql.nio.handler.ConnectionHeartBeatHandler;
import com.zhongan.dmds.net.mysql.nio.handler.DelegateResponseHandler;
import com.zhongan.dmds.net.mysql.nio.handler.NewConnectionRespHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 2019.01 完善配置热加载
 */
public abstract class PhysicalDatasource implements IPhysicalDatasource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDatasource.class);

  private final String name;
  private final int size;
  private final DBHostConfig config;
  private final ConMap conMap = new ConMap();
  private IDBHeartbeat heartbeat;
  private final boolean readNode;
  private volatile long heartbeatRecoveryTime;
  private final DataHostConfig hostConfig;
  private final ConnectionHeartBeatHandler conHeartBeatHanler = new ConnectionHeartBeatHandler();
  private IPhysicalDBPool dbPool;

  public PhysicalDatasource(DBHostConfig config, DataHostConfig hostConfig, boolean isReadNode) {
    this.size = config.getMaxCon();
    this.config = config;
    this.name = config.getHostName();
    this.hostConfig = hostConfig;
    this.heartbeat = this.createHeartBeat();
    this.readNode = isReadNode;
  }

  public boolean isMyConnection(BackendConnection con) {
    if (con instanceof MySQLConnection) {
      return ((MySQLConnection) con).getPool() == this;
    } else {
      return false;
    }
  }

  public abstract IDBHeartbeat createHeartBeat();

  public DataHostConfig getHostConfig() {
    return hostConfig;
  }

  public boolean isReadNode() {
    return readNode;
  }

  public int getSize() {
    return size;
  }

  public void setDbPool(IPhysicalDBPool dbPool) {
    this.dbPool = dbPool;
  }

  public IPhysicalDBPool getDbPool() {
    return dbPool;
  }

  public String getName() {
    return name;
  }

  public long getExecuteCount() {
    long executeCount = 0;
    for (ConQueue queue : conMap.getAllConQueue()) {
      executeCount += queue.getExecuteCount();

    }
    return executeCount;
  }

  public long getExecuteCountForSchema(String schema) {
    return conMap.getSchemaConQueue(schema).getExecuteCount();

  }

  public int getActiveCountForSchema(String schema) {
    return conMap.getActiveCountForSchema(schema, this);
  }

  public int getIdleCountForSchema(String schema) {
    ConQueue queue = conMap.getSchemaConQueue(schema);
    int total = 0;
    total += queue.getAutoCommitCons().size() + queue.getManCommitCons().size();
    return total;
  }

  public IDBHeartbeat getHeartbeat() {
    return heartbeat;
  }

  public int getIdleCount() {
    int total = 0;
    for (ConQueue queue : conMap.getAllConQueue()) {
      total += queue.getAutoCommitCons().size() + queue.getManCommitCons().size();
    }
    return total;
  }

  private boolean validSchema(String schema) {
    String theSchema = schema;
    return theSchema != null && !"".equals(theSchema) && !"snyn...".equals(theSchema);
  }

  private void checkIfNeedHeartBeat(LinkedList<BackendConnection> heartBeatCons, ConQueue queue,
      ConcurrentLinkedQueue<BackendConnection> checkLis, long hearBeatTime, long hearBeatTime2) {
    int maxConsInOneCheck = 10;
    Iterator<BackendConnection> checkListItor = checkLis.iterator();
    while (checkListItor.hasNext()) {
      BackendConnection con = checkListItor.next();
      if (con.isClosedOrQuit()) {
        checkListItor.remove();
        continue;
      }
      if (validSchema(con.getSchema())) {
        if (con.getLastTime() < hearBeatTime && heartBeatCons.size() < maxConsInOneCheck) {
          checkListItor.remove();
          // Heart beat check
          con.setBorrowed(true);
          heartBeatCons.add(con);
        }
      } else if (con.getLastTime() < hearBeatTime2) {
        // not valid schema conntion should close for idle
        // exceed 2*conHeartBeatPeriod
        checkListItor.remove();
        con.close(" heart beate idle ");
      }
    }
  }

  public int getIndex() {
    int currentIndex = 0;
    for (int i = 0; i < dbPool.getSources().length; i++) {
      IPhysicalDatasource writeHostDatasource = dbPool.getSources()[i];
      if (writeHostDatasource.getName().equals(getName())) {
        currentIndex = i;
        break;
      }
    }
    return currentIndex;
  }

  public boolean isSalveOrRead() {
    int currentIndex = getIndex();
    if (currentIndex != dbPool.getActivedIndex() || this.readNode) {
      return true;
    }
    return false;
  }

  /**
   * 检查空闲超时连接，将其close
   *
   * @param conHeartBeatPeriod 心跳检查时间间隔
   */
  public void heatBeatCheck(long timeout, long conHeartBeatPeriod) {
    int maxConsInOneCheck = 5;
    LinkedList<BackendConnection> heartBeatCons = new LinkedList<BackendConnection>();
    long hearBeatTime = TimeUtil.currentTimeMillis() - conHeartBeatPeriod;
    long hearBeatTime2 = TimeUtil.currentTimeMillis() - 2 * conHeartBeatPeriod;

    for (ConQueue queue : conMap.getAllConQueue()) {
      checkIfNeedHeartBeat(heartBeatCons, queue, queue.getAutoCommitCons(), hearBeatTime,
          hearBeatTime2);
      if (heartBeatCons.size() < maxConsInOneCheck) {
        checkIfNeedHeartBeat(heartBeatCons, queue, queue.getManCommitCons(), hearBeatTime,
            hearBeatTime2);
      } else if (heartBeatCons.size() >= maxConsInOneCheck) {
        break;
      }
    }

    if (!heartBeatCons.isEmpty()) {
      for (BackendConnection con : heartBeatCons) {
        conHeartBeatHanler.doHeartBeat(con, hostConfig.getHearbeatSQL());
      }
    }

    // check if there has timeouted heatbeat cons
    conHeartBeatHanler.abandTimeOuttedConns();
    int idleCons = this.getIdleCount();
    int activeCons = this.getActiveCount();
    int createCount = (hostConfig.getMinCon() - idleCons) / 3;

    // create if idle too little
    if ((createCount > 0) && (idleCons + activeCons < size) && (idleCons < hostConfig
        .getMinCon())) {

      createByIdleLitte(idleCons, createCount);

    } else if (idleCons > hostConfig.getMinCon()) {

      closeByIdleMany(idleCons - hostConfig.getMinCon());

    } else {
      int activeCount = this.getActiveCount();
      if (activeCount > size) {
        StringBuilder s = new StringBuilder().append(Alarms.DEFAULT)
            .append("DATASOURCE EXCEED [name=").append(name).append(",active=").append(activeCount)
            .append(",size=").append(size).append(']');
        LOGGER.warn(s.toString());
      }
    }
  }

  private void closeByIdleMany(int idleCloseCount) {
    LOGGER.info("too many idle cons ,close some for datasouce  " + name);

    List<BackendConnection> readyCloseCons = new ArrayList<BackendConnection>(idleCloseCount);

    for (ConQueue queue : conMap.getAllConQueue()) {
      readyCloseCons.addAll(queue.getIdleConsToClose(idleCloseCount));
      if (readyCloseCons.size() >= idleCloseCount) {
        break;
      }
    }

    for (BackendConnection idleCon : readyCloseCons) {
      if (idleCon.isBorrowed()) {
        LOGGER.warn("find idle con is using " + idleCon);
      }
      idleCon.close("too many idle con");
    }
  }

  private void createByIdleLitte(int idleCons, int createCount) {
    LOGGER.info("create connections ,because idle connection not enough ,current is " + idleCons
        + ", minCon is " + hostConfig.getMinCon() + " for " + name);
    NewConnectionRespHandler simpleHandler = new NewConnectionRespHandler();

    final String[] schemas = dbPool.getSchemas();
    for (int i = 0; i < createCount; i++) {
      if (this.getActiveCount() + this.getIdleCount() >= size) {
        break;
      }
      try {
        // creat new connection
        this.createNewConnection(simpleHandler, null, schemas[i % schemas.length], false);
      } catch (IOException e) {
        LOGGER.warn("create connection err " + e);
      }
    }
  }

  public int getActiveCount() {
    return this.conMap.getActiveCountForDs(this);
  }

  public int getConnectionNumByDataHost(String dataHost) {
    return ConMap.getConnectionNumByDataHost(dataHost);
  }

  public void clearCons(String reason) {
    this.conMap.clearConnections(reason, this);
  }

  public void startHeartbeat() {
    heartbeat.start();
  }

  public void stopHeartbeat() {
    heartbeat.stop();
  }

  public void doHeartbeat() {
    // 未到预定恢复时间，不执行心跳检测。
    if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
      return;
    }
    if (!heartbeat.isStop()) {
      try {
        heartbeat.heartbeat();
      } catch (Exception e) {
        LOGGER.error(name + " heartbeat error.", e);
      }
    }
  }

  private BackendConnection takeCon(BackendConnection conn, final ResponseHandler handler,
      final Object attachment,
      String schema) {
    conn.setBorrowed(true);
    if (!conn.getSchema().equals(schema)) {
      // need do schema syn in before sql send
      conn.setSchema(schema);
    }
    ConQueue queue = conMap.getSchemaConQueue(schema);
    queue.incExecuteCount();
    conn.setAttachment(attachment);
    conn.setLastTime(
        System.currentTimeMillis()); // 每次取连接的时候，更新下lasttime，防止在前端连接检查的时候，关闭连接，导致sql执行失败
    handler.connectionAcquired(conn);
    return conn;
  }

  private void createNewConnection(final ResponseHandler handler, final Object attachment,
      final String schema, final boolean isInit)
      throws IOException {
    // aysn create connection
    final String dataHost = this.hostConfig.getName();
    DmdsContext.getInstance().getBusinessExecutor().execute(new Runnable() {
      public void run() {
        try {
          createNewConnection(new DelegateResponseHandler(handler) {
            @Override
            public void connectionError(Throwable e, BackendConnection conn) {
              handler.connectionError(e, conn);
              if (isInit) {
                DmdsContext.getInstance().getInitConnectionLatch().countDown();
              }

            }

            @Override
            public void connectionAcquired(BackendConnection conn) {
              takeCon(conn, handler, attachment, schema);
              if (isInit) {
                DmdsContext.getInstance().getInitConnectionLatch().countDown();
              }
            }
          }, schema);

          ConMap.addConnectionNum(dataHost);

          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "创建连接：" + dataHost + "，当前连接数量：" + ConMap.getConnectionNumByDataHost(dataHost));
          }

        } catch (IOException e) {
          handler.connectionError(e, null);
        }
      }
    });
  }

  public void getConnection(String schema, boolean autocommit, final ResponseHandler handler,
      final Object attachment)
      throws IOException {
    getConnection(schema, autocommit, handler, attachment, false);
  }

  public void getConnection(String schema, boolean autocommit, final ResponseHandler handler,
      final Object attachment, boolean isInit)
      throws IOException {
    BackendConnection con = null;
    if (!isInit) {
      con = this.conMap.tryTakeCon(schema, autocommit);
    }

    if (con != null) {
      takeCon(con, handler, attachment, schema);
      return;
    } else {
      // gwd modified
      // 用datahost的链接数来判断
      int conNum = this.getConnectionNumByDataHost(this.hostConfig.getName());
      // 下一个连接大于最大连接数，判断的是物理RDS的连接
      if ((conNum + 1) > size) {
        LOGGER.error("the max connnection size can not be max than maxconnections");
        throw new IOException("the max connnection size can not be max than maxconnections");

      } else {
        LOGGER.info(
            "not idle connection in pool,create new connection for " + this.name + " of schema "
                + schema);
        createNewConnection(handler, attachment, schema, isInit);
      }
    }
  }

  private void returnCon(BackendConnection c) {
    c.setAttachment(null);
    c.setBorrowed(false);
    c.setLastTime(TimeUtil.currentTimeMillis());
    ConQueue queue = this.conMap.getSchemaConQueue(c.getSchema());

    boolean ok = false;
    if (c.isAutocommit()) {
      ok = queue.getAutoCommitCons().offer(c);
    } else {
      ok = queue.getManCommitCons().offer(c);
    }
    if (!ok) {
      LOGGER.warn("can't return to pool ,so close con " + c);
      c.close("can't return to pool ");
    }
  }

  public void releaseChannel(BackendConnection c) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("release channel " + c);
    }
    // release connection
    returnCon(c);
  }

  public void connectionClosed(BackendConnection conn) {
    ConQueue queue = this.conMap.getSchemaConQueue(conn.getSchema());
    if (queue != null) {
      queue.removeCon(conn);
      // guoweidong 关闭连接时 修改连接数
      ConMap.removeConnectionNum(this.hostConfig.getName());

      if (LOGGER.isDebugEnabled()) {
        printCon();
      }
    }
  }

  private void printCon() {
    LOGGER.debug("=========开始打印连接==========");
    Collection<ConQueue> queues = this.conMap.getAllConQueue();
    for (ConQueue conQueue : queues) {
      ConcurrentLinkedQueue<BackendConnection> backendCons = conQueue.getAutoCommitCons();
      for (BackendConnection con : backendCons) {
        LOGGER
            .info(this.getName() + ", Attachment:" + con.getAttachment() + ", host:" + con.getHost()
                + ", charset:" + con.getCharset() + ",id:" + con.getId() + ",schema:" + con
                .getSchema()
                + ",isBorrowed:" + con.isBorrowed());
      }
    }
    LOGGER.debug("=========结束打印连接==========");
  }

  public abstract void createNewConnection(ResponseHandler handler,
      String schema) throws IOException;

  public long getHeartbeatRecoveryTime() {
    return heartbeatRecoveryTime;
  }

  public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
    this.heartbeatRecoveryTime = heartbeatRecoveryTime;
  }

  public DBHostConfig getConfig() {
    return config;
  }
}
