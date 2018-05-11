/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model;

import com.zhongan.dmds.commons.contants.mysql.DmdsConstants;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Datahost is a group of DB servers which is synchronized with each other
 *
 * 2017.03 move switchType Constant to DmdsConstant
 */
public class DataHostConfig {

  private static final Pattern pattern = Pattern
      .compile("\\s*show\\s+slave\\s+status\\s*", Pattern.CASE_INSENSITIVE);
  private static final Pattern patternCluster = Pattern
      .compile("\\s*show\\s+status\\s+like\\s+'wsrep%'", Pattern.CASE_INSENSITIVE);
  private String name;
  private int maxCon = SystemConfig.DEFAULT_POOL_SIZE;
  private int minCon = 10;
  private int balance = DmdsConstants.BALANCE_NONE;
  private int writeType = DmdsConstants.WRITE_ONLYONE_NODE;
  private final String dbType;
  private final String dbDriver;
  private final DBHostConfig[] writeHosts;
  private final Map<Integer, DBHostConfig[]> readHosts;
  private String hearbeatSQL;
  private boolean isShowSlaveSql = false;
  private boolean isShowClusterSql = false;
  private String connectionInitSql;
  private int slaveThreshold = -1;
  private final int switchType;
  private String filters = "mergeStat";
  private long logTime = 300000;
  private boolean tempReadHostAvailable = false; // 如果写服务挂掉, 临时读服务是否继续可用

  public DataHostConfig(String name, String dbType, String dbDriver, DBHostConfig[] writeHosts,
      Map<Integer, DBHostConfig[]> readHosts, int switchType, int slaveThreshold,
      boolean tempReadHostAvailable) {
    super();
    this.name = name;
    this.dbType = dbType;
    this.dbDriver = dbDriver;
    this.writeHosts = writeHosts;
    this.readHosts = readHosts;
    this.switchType = switchType;
    this.slaveThreshold = slaveThreshold;
    this.tempReadHostAvailable = tempReadHostAvailable;
  }

  public boolean isTempReadHostAvailable() {
    return this.tempReadHostAvailable;
  }

  public int getSlaveThreshold() {
    return slaveThreshold;
  }

  public void setSlaveThreshold(int slaveThreshold) {
    this.slaveThreshold = slaveThreshold;
  }

  public int getSwitchType() {
    return switchType;
  }

  public String getConnectionInitSql() {
    return connectionInitSql;
  }

  public void setConnectionInitSql(String connectionInitSql) {
    this.connectionInitSql = connectionInitSql;
  }

  public int getWriteType() {
    return writeType;
  }

  public void setWriteType(int writeType) {
    this.writeType = writeType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isShowSlaveSql() {
    return isShowSlaveSql;
  }

  public int getMaxCon() {
    return maxCon;
  }

  public void setMaxCon(int maxCon) {
    this.maxCon = maxCon;
  }

  public int getMinCon() {
    return minCon;
  }

  public void setMinCon(int minCon) {
    this.minCon = minCon;
  }

  public int getBalance() {
    return balance;
  }

  public void setBalance(int balance) {
    this.balance = balance;
  }

  public String getDbType() {
    return dbType;
  }

  public String getDbDriver() {
    return dbDriver;
  }

  public DBHostConfig[] getWriteHosts() {
    return writeHosts;
  }

  public Map<Integer, DBHostConfig[]> getReadHosts() {
    return readHosts;
  }

  public String getHearbeatSQL() {
    return hearbeatSQL;
  }

  public void setHearbeatSQL(String heartbeatSQL) {
    this.hearbeatSQL = heartbeatSQL;
    Matcher matcher = pattern.matcher(heartbeatSQL);
    if (matcher.find()) {
      isShowSlaveSql = true;
    }
    Matcher matcher2 = patternCluster.matcher(heartbeatSQL);
    if (matcher2.find()) {
      isShowClusterSql = true;
    }
  }

  public String getFilters() {
    return filters;
  }

  public void setFilters(String filters) {
    this.filters = filters;
  }

  public long getLogTime() {
    return logTime;
  }

  public boolean isShowClusterSql() {
    return this.isShowClusterSql;
  }

  public void setLogTime(long logTime) {
    this.logTime = logTime;
  }
}