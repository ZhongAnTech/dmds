/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route;

import com.zhongan.dmds.commons.contants.mysql.DmdsConstants;
import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.commons.util.TbSeqUtil;
import com.zhongan.dmds.mpp.LoadData;
import org.apache.commons.lang3.StringUtils;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.Serializable;
import java.util.Map;

/**
 * dmds支持分表
 * 优化多表查询
 */
public final class RouteResultsetNode implements Serializable, Comparable<RouteResultsetNode> {

  private static final long serialVersionUID = 1L;
  private String name; // 数据节点名称
  private String statement; // 执行的语句
  private final String srcStatement;
  private final int sqlType;
  private volatile boolean canRunInReadDB;
  private final boolean hasBlanceFlag;
  private boolean callStatement = false; // 处理call关键字
  private int limitStart;
  private int limitSize;
  private int totalNodeSize = 0; // 方便后续jdbc批量获取扩展
  private Procedure procedure;
  private LoadData loadData;
  private String tableName; // 分表的数据表名
  private Map<String, String> hintMap;
  private String[] shardingTableNames;

  private volatile boolean isExecuted = false;    //是否已经执行完
  private PropertyChangeSupport changeSupport = new PropertyChangeSupport(this);

  public boolean isExecuted() {
    return isExecuted;
  }

  public void setExecuted(boolean isExecuted) {
    boolean oldValue = this.isExecuted;
    this.isExecuted = isExecuted;

    if (changeSupport.getPropertyChangeListeners().length >= 1 && oldValue == false
        && this.isExecuted) {
      changeSupport.firePropertyChange("executed", oldValue, this.isExecuted);
    }
  }

  public void addPropertyChangeListener(PropertyChangeListener listener) {
    changeSupport.addPropertyChangeListener(listener);
  }

  public void removePropertyChangeListener(PropertyChangeListener listener) {
    changeSupport.removePropertyChangeListener(listener);
  }

  public RouteResultsetNode(String name, int sqlType, String srcStatement) {
    this.name = name;
    this.srcStatement = srcStatement;
    this.limitStart = 0;
    this.limitSize = -1;
    this.sqlType = sqlType;
    this.statement = srcStatement;
    this.canRunInReadDB = (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW);
    this.hasBlanceFlag = (statement != null) && statement.startsWith("/*balance*/");

    // 如果是 dataNode and tableName 分隔替换
    if (name.indexOf(DmdsConstants.seq_db_tb) != -1) {
      String[] devInfo = name.split(DmdsConstants.seq_db_tb);
      if (devInfo.length >= 2) {
        this.name = devInfo[0];
        this.shardingTableNames = new String[devInfo.length - 1];
        System.arraycopy(devInfo, 1, shardingTableNames, 0, shardingTableNames.length);
      }
    }
    this.rebuildShardingSQL();
  }

  private void rebuildShardingSQL() {
    if (shardingTableNames != null && shardingTableNames.length > 0) {
      // 最后一张表为tablename, 兼容之前的逻辑
      this.tableName = shardingTableNames[shardingTableNames.length - 1];
      this.statement = TbSeqUtil.replaceShardingTableNames(statement, shardingTableNames);
    }
  }

  public Map<String, String> getHintMap() {
    return hintMap;
  }

  public void setHintMap(Map<String, String> hintMap) {
    this.hintMap = hintMap;
  }

  public void setStatement(String statement) {
    this.statement = statement;
    this.rebuildShardingSQL();
  }

  public void setCanRunInReadDB(boolean canRunInReadDB) {
    this.canRunInReadDB = canRunInReadDB;
  }

  public boolean getCanRunInReadDB() {
    return this.canRunInReadDB;
  }

  public void resetStatement() {
    this.statement = srcStatement;
  }

  public boolean canRunnINReadDB(boolean autocommit) {
    return canRunInReadDB && autocommit && !hasBlanceFlag
        || canRunInReadDB && !autocommit && hasBlanceFlag;
  }

  public Procedure getProcedure() {
    return procedure;
  }

  public void setProcedure(Procedure procedure) {
    this.procedure = procedure;
  }

  public boolean isCallStatement() {
    return callStatement;
  }

  public void setCallStatement(boolean callStatement) {
    this.callStatement = callStatement;
  }

  public String getName() {
    return name;
  }

  public int getSqlType() {
    return sqlType;
  }

  public String getStatement() {
    return statement;
  }

  public int getLimitStart() {
    return limitStart;
  }

  public void setLimitStart(int limitStart) {
    this.limitStart = limitStart;
  }

  public int getLimitSize() {
    return limitSize;
  }

  public void setLimitSize(int limitSize) {
    this.limitSize = limitSize;
  }

  public int getTotalNodeSize() {
    return totalNodeSize;
  }

  public void setTotalNodeSize(int totalNodeSize) {
    this.totalNodeSize = totalNodeSize;
  }

  public LoadData getLoadData() {
    return loadData;
  }

  public void setLoadData(LoadData loadData) {
    this.loadData = loadData;
  }

  @Override
  public int hashCode() {
    return new String(this.name + this.tableName).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof RouteResultsetNode) {
      RouteResultsetNode rrn = (RouteResultsetNode) obj;
      if (equals(name, rrn.getName()) && equals(this.tableName, rrn.getTableName())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append(name);
    if (StringUtils.isNotBlank(this.tableName)) {
      s.append(DmdsConstants.seq_db_tb);
      s.append(this.tableName);
    }
    s.append('{').append(statement).append('}');
    return s.toString();
  }

  private static boolean equals(String str1, String str2) {
    if (str1 == null) {
      return str2 == null;
    }
    return str1.equals(str2);
  }

  public boolean isModifySQL() {
    return !canRunInReadDB;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public int compareTo(RouteResultsetNode obj) {
    if (obj == null) {
      return 1;
    }
    if (this.name == null) {
      return -1;
    }

    if (obj.name == null) {
      return 1;
    }
    String value = new String(this.name + this.tableName);
    String objValue = new String(obj.name + obj.tableName);
    return value.compareTo(objValue);
  }
}
