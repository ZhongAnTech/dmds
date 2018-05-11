/*
 * Copyright (C) 2016-2020 zhongan.com
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.util;

import com.zhongan.dmds.commons.contants.mysql.DmdsConstants;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 分表Node 工具类
 * dmds 支持分表
 *
 * @author gwd
 */
public class TableDivNode {

  private String node;
  private String table;
  private boolean isMatched = false;

  public TableDivNode(String node, String table) {
    this.node = node;
    this.table = table;
  }

  public TableDivNode(String node) {
    if (node.indexOf(DmdsConstants.seq_db_tb) > 0) {
      String[] nodes = node.split("#");
      this.node = nodes[0];
      this.table = nodes[1];
    } else {
      this.node = node;
    }
  }

  public static List<TableDivNode> getNodeList(Set<String> nodeSet) {
    List<TableDivNode> list = new ArrayList<TableDivNode>();
    for (String node : nodeSet) {
      list.add(new TableDivNode(node));
    }
    return list;
  }

  public static List<TableDivNode> addNodeSet(List<TableDivNode> list, Set<String> nodeSet) {

    List<TableDivNode> retList = new ArrayList<TableDivNode>();

    for (TableDivNode tableDivNode : list) {
      String oldTable = tableDivNode.getTable();

      for (String nodeName : nodeSet) {
        TableDivNode newTableDivNode = new TableDivNode(nodeName);
        if (newTableDivNode.getNode().equals(tableDivNode.getNode())) {
          String newTable = newTableDivNode.getTable();
          if (oldTable == null && newTable != null) {
            addTableDivNode(retList, tableDivNode.getNode(), newTable);
          } else if (oldTable != null && newTable != null) {
            if (oldTable.equals(newTable)) {
              addTableDivNode(retList, tableDivNode.getNode(), newTable);
            } else if (oldTable.substring(oldTable.length() - 4)
                .equals(newTable.substring(newTable.length() - 4))) {
              addTableDivNode(retList, tableDivNode.getNode(), oldTable + "#" + newTable);
            }
          } else if (oldTable != null && newTable == null) {
            addTableDivNode(retList, tableDivNode.getNode(), oldTable);
          } else if (oldTable == null && newTable == null) {
            addTableDivNode(retList, tableDivNode.getNode(), null);
          }
        }
      }
    }
    return retList;
  }

  private static void addTableDivNode(List<TableDivNode> list, String node, String table) {
    TableDivNode tableDivNode = new TableDivNode(node, table);
    list.add(tableDivNode);
  }

  // 转换成nodeSet
  public static Set<String> toNodeSet(List<TableDivNode> list) {
    Set<String> nodeSet = new HashSet<String>(list.size());
    for (TableDivNode node : list) {
      nodeSet.add(node.toString());
    }
    return nodeSet;
  }

  public String toString() {
    if (StringUtils.isBlank(node) && StringUtils.isBlank(table)) {
      return "";
    } else if (StringUtils.isNotBlank(node) && StringUtils.isBlank(table)) {
      return node;
    } else {
      return node + DmdsConstants.seq_db_tb + table;
    }
  }

  public String getNode() {
    return node;
  }

  public void setNode(String node) {
    this.node = node;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public boolean isMatched() {
    return isMatched;
  }

  public void setMatched(boolean isMatched) {
    this.isMatched = isMatched;
  }

}
