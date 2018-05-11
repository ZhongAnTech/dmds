/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.backend;

import com.zhongan.dmds.commons.contants.mysql.DmdsConstants;
import com.zhongan.dmds.config.Alarms;
import com.zhongan.dmds.config.model.DataHostConfig;
import com.zhongan.dmds.core.*;
import com.zhongan.dmds.net.mysql.nio.handler.GetConnectionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Move constants to dmdsConstant
 * Remove support for WRITE_ONLYONE_NODE
 */
public class PhysicalDBPool implements IPhysicalDBPool {

  protected static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDBPool.class);

  private final String hostName;

  protected IPhysicalDatasource[] writeSources;
  protected Map<Integer, IPhysicalDatasource[]> readSources;

  public volatile int activedIndex;
  protected volatile boolean initSuccess;

  protected final ReentrantLock switchLock = new ReentrantLock();
  private final Collection<IPhysicalDatasource> allDs;
  private final int banlance;
  private final int writeType;
  private final Random random = new Random();
  private final Random wnrandom = new Random();
  private String[] schemas;
  private final DataHostConfig dataHostConfig;

  public PhysicalDBPool(String name, DataHostConfig conf, IPhysicalDatasource[] writeSources,
      Map<Integer, IPhysicalDatasource[]> readSources, int balance, int writeType) {

    this.hostName = name;
    this.dataHostConfig = conf;
    this.writeSources = writeSources;
    this.banlance = balance;
    this.writeType = writeType;

    Iterator<Map.Entry<Integer, IPhysicalDatasource[]>> entryItor = readSources.entrySet()
        .iterator();
    while (entryItor.hasNext()) {
      IPhysicalDatasource[] values = entryItor.next().getValue();
      if (values.length == 0) {
        entryItor.remove();
      }
    }

    this.readSources = readSources;
    this.allDs = this.genAllDataSources();

    LOGGER.info("total resouces of dataHost " + this.hostName + " is :" + allDs.size());

    setDataSourceProps();
  }

  /*
   * (non-Javadoc)
   *
   * @see com.zhongan.dmds.net.backend.IPhysicalDBPool#getWriteType()
   */
  @Override
  public int getWriteType() {
    return writeType;
  }

  private void setDataSourceProps() {
    for (IPhysicalDatasource ds : this.allDs) {
      ds.setDbPool(this);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.zhongan.dmds.net.backend.IPhysicalDBPool#findDatasouce(com.zhongan.dmds.
   * core.BackendConnection)
   */
  @Override
  public IPhysicalDatasource findDatasouce(BackendConnection exitsCon) {
    for (IPhysicalDatasource ds : this.allDs) {
      if (ds.isReadNode() == exitsCon.isFromSlaveDB()) {
        if (ds.isMyConnection(exitsCon)) {
          return ds;
        }
      }
    }

    LOGGER.warn("can't find connection in pool " + this.hostName + " con:" + exitsCon);
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see com.zhongan.dmds.net.backend.IPhysicalDBPool#getHostName()
   */
  @Override
  public String getHostName() {
    return hostName;
  }

  /*
   * (non-Javadoc)
   *
   * @see com.zhongan.dmds.net.backend.IPhysicalDBPool#getSources()
   */
  @Override
  public IPhysicalDatasource[] getSources() {
    return writeSources;
  }

  /*
   * (non-Javadoc)
   *
   * @see com.zhongan.dmds.net.backend.IPhysicalDBPool#getSource()
   */
  @Override
  public IPhysicalDatasource getSource() {

    switch (writeType) {
      case DmdsConstants.WRITE_ONLYONE_NODE: {
        return writeSources[activedIndex];
      }
      case DmdsConstants.WRITE_RANDOM_NODE: {

        int index = Math.abs(wnrandom.nextInt()) % writeSources.length;
        IPhysicalDatasource result = writeSources[index];
        if (!this.isAlive(result)) {

          // find all live nodes
          ArrayList<Integer> alives = new ArrayList<Integer>(writeSources.length - 1);
          for (int i = 0; i < writeSources.length; i++) {
            if (i != index) {
              if (this.isAlive(writeSources[i])) {
                alives.add(i);
              }
            }
          }

          if (alives.isEmpty()) {
            result = writeSources[0];
          } else {
            // random select one
            index = Math.abs(wnrandom.nextInt()) % alives.size();
            result = writeSources[alives.get(index)];

          }
        }

        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "select write source " + result.getName() + " for dataHost:" + this.getHostName());
        }
        return result;
      }
      default: {
        throw new java.lang.IllegalArgumentException(
            "writeType is " + writeType + " ,so can't return one write datasource ");
      }
    }

  }

  /*
   * (non-Javadoc)
   *
   * @see com.zhongan.dmds.net.backend.IPhysicalDBPool#getActivedIndex()
   */
  @Override
  public int getActivedIndex() {
    return activedIndex;
  }

  /*
   * (non-Javadoc)
   *
   * @see com.zhongan.dmds.net.backend.IPhysicalDBPool#isInitSuccess()
   */
  @Override
  public boolean isInitSuccess() {
    return initSuccess;
  }

  /*
   * (non-Javadoc)
   *
   * @see com.zhongan.dmds.net.backend.IPhysicalDBPool#next(int)
   */
  @Override
  public int next(int i) {
    if (checkIndex(i)) {
      return (++i == writeSources.length) ? 0 : i;
    } else {
      return 0;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see com.zhongan.dmds.net.backend.IPhysicalDBPool#switchSource(int, boolean,
   * java.lang.String)
   */
  @Override
  public boolean switchSource(int newIndex, boolean isAlarm, String reason) {
    if (this.writeType != DmdsConstants.WRITE_ONLYONE_NODE || !checkIndex(newIndex)) {
      return false;
    }

    final ReentrantLock lock = this.switchLock;
    lock.lock();
    try {
      int current = activedIndex;
      if (current != newIndex) {

        // switch index
        activedIndex = newIndex;

        // init again
        this.init(activedIndex);

        // clear all connections
        this.getSources()[current].clearCons("switch datasource");

        // write log
        LOGGER.warn(switchMessage(current, newIndex, false, reason));

        return true;
      }
    } finally {
      lock.unlock();
    }
    return false;
  }

  private String switchMessage(int current, int newIndex, boolean alarm, String reason) {
    StringBuilder s = new StringBuilder();
    if (alarm) {
      s.append(Alarms.DATANODE_SWITCH);
    }
    s.append("[Host=").append(hostName).append(",result=[").append(current).append("->");
    s.append(newIndex).append("],reason=").append(reason).append(']');
    return s.toString();
  }

  private int loop(int i) {
    return i < writeSources.length ? i : (i - writeSources.length);
  }

  @Override
  public void init(int index) {

    if (!checkIndex(index)) {
      index = 0;
    }

    int active = -1;
    for (int i = 0; i < writeSources.length; i++) {
      int j = loop(i + index);
      if (initSource(j, writeSources[j])) {

        // 不切换-1时，如果主写挂了 不允许切换过去
        if (dataHostConfig.getSwitchType() == DmdsConstants.NOT_SWITCH_DS && j > 0) {
          break;
        }

        active = j;
        activedIndex = active;
        initSuccess = true;
        LOGGER.info(getMessage(active, " init success"));

        if (this.writeType == DmdsConstants.WRITE_ONLYONE_NODE) {
          // only init one write datasource
          // TODO duantneed
          // DmdsServer.getInstance().saveDataHostIndex(hostName, activedIndex);
          break;
        }
      }
    }

    if (!checkIndex(active)) {
      initSuccess = false;
      StringBuilder s = new StringBuilder();
      s.append(Alarms.DEFAULT).append(hostName).append(" init failure");
      LOGGER.error(s.toString());
    }
  }

  private boolean checkIndex(int i) {
    return i >= 0 && i < writeSources.length;
  }

  private String getMessage(int index, String info) {
    return new StringBuilder().append(hostName).append(" index:").append(index).append(info)
        .toString();
  }

  private boolean initSource(int index, IPhysicalDatasource ds) {
    int initSize = ds.getConfig().getMinCon();

    LOGGER.info(
        "init backend myqsl source , create connections total " + initSize + " for " + ds.getName()
            + " index :" + index);

    CopyOnWriteArrayList<BackendConnection> list = new CopyOnWriteArrayList<BackendConnection>();
    GetConnectionHandler getConHandler = new GetConnectionHandler(list, initSize);

    for (int i = 0; i < initSize; i++) {
      try {
        LOGGER.info("initSource:::::schemas:" + schemas + ",hostName:" + this.hostName);
        ds.getConnection(this.schemas[i % schemas.length], true, getConHandler, null, true);
      } catch (Exception e) {
        LOGGER.warn(getMessage(index, " init connection error."), e);
      }
    }
    long timeOut = System.currentTimeMillis() + 60 * 1000;

    // waiting for finish
    while (!getConHandler.finished() && (System.currentTimeMillis() < timeOut)) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOGGER.error("initError", e);
      }
    }
    LOGGER.info("init result :" + getConHandler.getStatusInfo());
    return !list.isEmpty();
  }

  @Override
  public void doHeartbeat() {
    if (writeSources == null || writeSources.length == 0) {
      return;
    }

    for (IPhysicalDatasource source : this.allDs) {
      if (source != null) {
        source.doHeartbeat();
      } else {
        StringBuilder s = new StringBuilder();
        s.append(Alarms.DEFAULT).append(hostName).append(" current dataSource is null!");
        LOGGER.error(s.toString());
      }
    }

  }

  @Override
  public void heartbeatCheck(long idleCheckPeriod) {
    for (IPhysicalDatasource ds : allDs) {
      // only readnode or all write node or writetype=WRITE_ONLYONE_NODE
      // and current write node will check
      if (ds != null
          && (ds.getHeartbeat().getStatus() == IDBHeartbeat.OK_STATUS)
          && (ds.isReadNode()
          || (this.writeType != DmdsConstants.WRITE_ONLYONE_NODE)
          || (this.writeType == DmdsConstants.WRITE_ONLYONE_NODE
          && ds == this.getSource()))) {
        ds.heatBeatCheck(ds.getConfig().getIdleTimeout(), idleCheckPeriod);
      }
    }
  }

  @Override
  public void startHeartbeat() {
    for (IPhysicalDatasource source : this.allDs) {
      source.startHeartbeat();
    }
  }

  @Override
  public void stopHeartbeat() {
    for (IPhysicalDatasource source : this.allDs) {
      source.stopHeartbeat();
    }
  }

  @Override
  public void clearDataSources(String reason) {
    LOGGER.info("clear datasours of pool " + this.hostName);
    for (IPhysicalDatasource source : this.allDs) {
      LOGGER.info("clear datasoure of pool  " + this.hostName + " ds:" + source.getConfig());
      source.clearCons(reason);
      source.stopHeartbeat();
    }
  }

  @Override
  public Collection<IPhysicalDatasource> genAllDataSources() {

    LinkedList<IPhysicalDatasource> allSources = new LinkedList<IPhysicalDatasource>();
    for (IPhysicalDatasource ds : writeSources) {
      if (ds != null) {
        allSources.add(ds);
      }
    }

    for (IPhysicalDatasource[] dataSources : this.readSources.values()) {
      for (IPhysicalDatasource ds : dataSources) {
        if (ds != null) {
          allSources.add(ds);
        }
      }
    }
    return allSources;
  }

  @Override
  public Collection<IPhysicalDatasource> getAllDataSources() {
    return this.allDs;
  }

  @Override
  public void getRWBanlanceCon(String schema, boolean autocommit, ResponseHandler handler,
      Object attachment,
      String database) throws Exception {

    IPhysicalDatasource theNode = null;
    ArrayList<IPhysicalDatasource> okSources = null;
    switch (banlance) {
      case DmdsConstants.BALANCE_ALL_BACK: {
        // all read nodes and the standard by masters
        okSources = getAllActiveRWSources(true, false, checkSlaveSynStatus());
        if (okSources.isEmpty()) {
          theNode = this.getSource();

        } else {
          theNode = randomSelect(okSources);
        }
        break;
      }
      case DmdsConstants.BALANCE_ALL: {
        okSources = getAllActiveRWSources(true, true, checkSlaveSynStatus());
        theNode = randomSelect(okSources);
        break;
      }
      case DmdsConstants.BALANCE_ALL_READ: {
        okSources = getAllActiveRWSources(false, false, checkSlaveSynStatus());
        theNode = randomSelect(okSources);
        break;
      }
      case DmdsConstants.BALANCE_NONE:
      default:
        // return default write data source
        theNode = this.getSource();
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER
          .debug("select read source " + theNode.getName() + " for dataHost:" + this.getHostName());
    }
    theNode.getConnection(schema, autocommit, handler, attachment);
  }

  private boolean checkSlaveSynStatus() {
    return (dataHostConfig.getSlaveThreshold() != -1)
        && (dataHostConfig.getSwitchType() == DmdsConstants.SYN_STATUS_SWITCH_DS);
  }

  @Override
  public IPhysicalDatasource randomSelect(ArrayList<IPhysicalDatasource> okSources) {
    if (okSources.isEmpty()) {
      return this.getSource();

    } else {
      int length = okSources.size(); // 总个数
      int totalWeight = 0; // 总权重
      boolean sameWeight = true; // 权重是否都一样
      for (int i = 0; i < length; i++) {
        int weight = okSources.get(i).getConfig().getWeight();
        totalWeight += weight; // 累计总权重
        if (sameWeight && i > 0 && weight != okSources.get(i - 1).getConfig()
            .getWeight()) { // 计算所有权重是否一样
          sameWeight = false;
        }
      }

      if (totalWeight > 0 && !sameWeight) {
        // 如果权重不相同且权重大于0则按总权重数随机
        int offset = random.nextInt(totalWeight);
        // 并确定随机值落在哪个片断上
        for (int i = 0; i < length; i++) {
          offset -= okSources.get(i).getConfig().getWeight();
          if (offset < 0) {
            return okSources.get(i);
          }
        }
      }

      // 如果权重相同或权重为0则均等随机
      return okSources.get(random.nextInt(length));
    }
  }

  @Override
  public int getBalance() {
    return banlance;
  }

  private boolean isAlive(IPhysicalDatasource theSource) {
    return (theSource.getHeartbeat().getStatus() == IDBHeartbeat.OK_STATUS);

  }

  private boolean canSelectAsReadNode(IPhysicalDatasource theSource) {

    if (theSource.getHeartbeat().getSlaveBehindMaster() == null
        || theSource.getHeartbeat().getDbSynStatus() == IDBHeartbeat.DB_SYN_ERROR) {
      return false;
    }

    return (theSource.getHeartbeat().getDbSynStatus() == IDBHeartbeat.DB_SYN_NORMAL)
        && (theSource.getHeartbeat().getSlaveBehindMaster() < this.dataHostConfig
        .getSlaveThreshold());

  }

  /**
   * return all backup write sources
   *
   * @param includeWriteNode         if include write nodes
   * @param includeCurWriteNode      if include current active write node. invalid when
   *                                 <code>includeWriteNode<code> is false
   * @param filterWithSlaveThreshold
   * @return
   */
  private ArrayList<IPhysicalDatasource> getAllActiveRWSources(boolean includeWriteNode,
      boolean includeCurWriteNode,
      boolean filterWithSlaveThreshold) {

    int curActive = activedIndex;
    ArrayList<IPhysicalDatasource> okSources = new ArrayList<IPhysicalDatasource>(
        this.allDs.size());

    for (int i = 0; i < this.writeSources.length; i++) {
      IPhysicalDatasource theSource = writeSources[i];
      if (isAlive(theSource)) {// write node is active

        if (includeWriteNode) {
          if (i == curActive && includeCurWriteNode == false) {
            // not include cur active source
          } else if (filterWithSlaveThreshold) {

            if (canSelectAsReadNode(theSource)) {
              okSources.add(theSource);
            } else {
              continue;
            }

          } else {
            okSources.add(theSource);
          }
        }

        if (!readSources.isEmpty()) {

          // check all slave nodes
          IPhysicalDatasource[] allSlaves = this.readSources.get(i);
          if (allSlaves != null) {
            for (IPhysicalDatasource slave : allSlaves) {
              if (isAlive(slave)) {

                if (filterWithSlaveThreshold) {
                  if (canSelectAsReadNode(slave)) {
                    okSources.add(slave);
                  } else {
                    continue;
                  }

                } else {
                  okSources.add(slave);
                }
              }
            }
          }
        }

      } else {

        // 如果写节点不OK, 也要保证临时的读服务正常
        if (this.dataHostConfig.isTempReadHostAvailable()) {
          if (!readSources.isEmpty()) {
            // check all slave nodes
            IPhysicalDatasource[] allSlaves = this.readSources.get(i);
            if (allSlaves != null) {
              for (IPhysicalDatasource slave : allSlaves) {
                if (isAlive(slave)) {

                  if (filterWithSlaveThreshold) {
                    if (canSelectAsReadNode(slave)) {
                      okSources.add(slave);
                    } else {
                      continue;
                    }

                  } else {
                    okSources.add(slave);
                  }
                }
              }
            }
          }
        }
      }

    }
    return okSources;
  }

  @Override
  public String[] getSchemas() {
    return schemas;
  }

  @Override
  public void setSchemas(String[] mySchemas) {
    this.schemas = mySchemas;
  }

}