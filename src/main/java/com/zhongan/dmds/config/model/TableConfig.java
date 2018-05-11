/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model;

import com.zhongan.dmds.commons.util.SplitUtil;
import com.zhongan.dmds.config.model.rule.RuleConfig;

import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

public class TableConfig {

  public static final int TYPE_GLOBAL_TABLE = 1;
  public static final int TYPE_GLOBAL_DEFAULT = 0;
  private final String name;
  private final String primaryKey;
  private final boolean autoIncrement;
  private final boolean needAddLimit;
  private final Set<String> dbTypes;
  private final int tableType;
  private final ArrayList<String> dataNodes;
  private final RuleConfig rule;
  private final String partitionColumn;
  private final boolean ruleRequired;
  private final TableConfig parentTC;
  private final boolean childTable;
  private final String joinKey;
  private final String parentKey;
  private final String locateRTableKeySql;
  // only has one level of parent
  private final boolean secondLevel;
  private final boolean partionKeyIsPrimaryKey;
  private final Random rand = new Random();

  public TableConfig(String name, String primaryKey, boolean autoIncrement, boolean needAddLimit,
      int tableType,
      String dataNode, Set<String> dbType, RuleConfig rule, boolean ruleRequired,
      TableConfig parentTC,
      boolean isChildTable, String joinKey, String parentKey) {
    if (name == null) {
      throw new IllegalArgumentException("table name is null");
    } else if (dataNode == null) {
      throw new IllegalArgumentException("dataNode name is null");
    }
    this.primaryKey = primaryKey;
    this.autoIncrement = autoIncrement;
    this.needAddLimit = needAddLimit;
    this.tableType = tableType;
    this.dbTypes = dbType;
    if (ruleRequired && rule == null) {
      throw new IllegalArgumentException("ruleRequired but rule is null");
    }

    this.name = name.toUpperCase();
    String theDataNodes[] = SplitUtil.split(dataNode, ',', '$', '-');

    if (theDataNodes == null || theDataNodes.length <= 0) {
      throw new IllegalArgumentException("invalid table dataNodes: " + dataNode);
    }
    dataNodes = new ArrayList<String>(theDataNodes.length);
    for (String dn : theDataNodes) {
      dataNodes.add(dn);
    }
    this.rule = rule;
    this.partitionColumn = (rule == null) ? null : rule.getColumn();
    partionKeyIsPrimaryKey =
        (partitionColumn == null) ? primaryKey == null : partitionColumn.equals(primaryKey);
    this.ruleRequired = ruleRequired;
    this.childTable = isChildTable;
    this.parentTC = parentTC;
    this.joinKey = joinKey;
    this.parentKey = parentKey;
    if (parentTC != null) {
      locateRTableKeySql = genLocateRootParentSQL();
      secondLevel = (parentTC.parentTC == null);
    } else {
      locateRTableKeySql = null;
      secondLevel = false;
    }
  }

  public String getPrimaryKey() {
    return primaryKey;
  }

  public Set<String> getDbTypes() {
    return dbTypes;
  }

  public boolean isAutoIncrement() {
    return autoIncrement;
  }

  public boolean isNeedAddLimit() {
    return needAddLimit;
  }

  public boolean isSecondLevel() {
    return secondLevel;
  }

  public String getLocateRTableKeySql() {
    return locateRTableKeySql;
  }

  public boolean isGlobalTable() {
    return this.tableType == TableConfig.TYPE_GLOBAL_TABLE;
  }

  public String genLocateRootParentSQL() {
    TableConfig tb = this;
    StringBuilder tableSb = new StringBuilder();
    StringBuilder condition = new StringBuilder();
    TableConfig prevTC = null;
    int level = 0;
    String latestCond = null;
    while (tb.parentTC != null) {
      tableSb.append(tb.parentTC.name).append(',');
      String relation = null;
      if (level == 0) {
        latestCond = " " + tb.parentTC.getName() + '.' + tb.parentKey + "=";
      } else {
        relation = tb.parentTC.getName() + '.' + tb.parentKey + '=' + tb.name + '.' + tb.joinKey;
        condition.append(relation).append(" AND ");
      }
      level++;
      prevTC = tb;
      tb = tb.parentTC;
    }
    String sql = "SELECT " + prevTC.parentTC.name + '.' + prevTC.parentKey + " FROM "
        + tableSb.substring(0, tableSb.length() - 1) + " WHERE "
        + ((level < 2) ? latestCond : condition.toString() + latestCond);
    return sql;

  }

  public String getPartitionColumn() {
    return partitionColumn;
  }

  public int getTableType() {
    return tableType;
  }

  /**
   * get root parent
   *
   * @return
   */
  public TableConfig getRootParent() {
    if (parentTC == null) {
      return null;
    }
    TableConfig preParent = parentTC;
    TableConfig parent = preParent.getParentTC();

    while (parent != null) {
      preParent = parent;
      parent = parent.getParentTC();
    }
    return preParent;
  }

  public TableConfig getParentTC() {
    return parentTC;
  }

  public boolean isChildTable() {
    return childTable;
  }

  public String getJoinKey() {
    return joinKey;
  }

  public String getParentKey() {
    return parentKey;
  }

  /**
   * @return upper-case
   */
  public String getName() {
    return name;
  }

  public ArrayList<String> getDataNodes() {
    return dataNodes;
  }

  public String getRandomDataNode() {
    int index = Math.abs(rand.nextInt()) % dataNodes.size();
    return dataNodes.get(index);
  }

  public boolean isRuleRequired() {
    return ruleRequired;
  }

  public RuleConfig getRule() {
    return rule;
  }

  public boolean primaryKeyIsPartionKey() {
    return partionKeyIsPrimaryKey;
  }

}