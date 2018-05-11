/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid.impl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.zhongan.dmds.commons.contants.mysql.DmdsConstants;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.commons.util.TbSeqUtil;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.TableConfig;
import com.zhongan.dmds.config.model.rule.RuleAlgorithm;
import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.route.RouteResultsetNode;
import com.zhongan.dmds.sqlParser.druid.DmdsSchemaStatVisitor;
import com.zhongan.dmds.sqlParser.druid.RouteCalculateUnit;
import com.zhongan.dmds.sqlParser.util.DmdsRouterUtil;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DruidInsertParser extends DefaultDruidParser {

  @Override
  public void visitorParse(RouteResultset rrs, SQLStatement stmt, DmdsSchemaStatVisitor visitor)
      throws SQLNonTransientException {

  }

  /**
   * 考虑因素：批量、是否分片
   */
  @Override
  public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt)
      throws SQLNonTransientException {
    MySqlInsertStatement insert = (MySqlInsertStatement) stmt;
    String tableName = StringUtil.removeBackquote(insert.getTableName().getSimpleName())
        .toUpperCase();

    ctx.addTable(tableName);
    // 整个schema都不分库或者该表不拆分
    if (DmdsRouterUtil.isNoSharding(schema, tableName)) {
      DmdsRouterUtil.routeForTableMeta(rrs, schema, tableName, rrs.getStatement());
      rrs.setFinishedRoute(true);
      return;
    }

    TableConfig tc = schema.getTables().get(tableName);
    if (tc == null) {
      String msg = "can't find table define in schema " + tableName + " schema:" + schema.getName();
      LOGGER.warn(msg);
      throw new SQLNonTransientException(msg);
    } else {
      String partitionColumn = tc.getPartitionColumn();
      if (partitionColumn != null) {// 分片表
        // 拆分表必须给出column list,否则无法寻找分片字段的值
        if (insert.getColumns() == null || insert.getColumns().size() == 0) {
          throw new SQLSyntaxErrorException("partition table, insert must provide ColumnList");
        }
        // 批量insert
        if (isMultiInsert(insert)) {
          parserBatchInsert(schema, rrs, partitionColumn, tableName, insert);
        } else {
          parserSingleInsert(schema, rrs, partitionColumn, tableName, insert);
        }
      }
    }
  }

  /**
   * 是否为批量插入：insert into ...values (),()...或 insert into ...select.....
   *
   * @param insertStmt
   * @return
   */
  private boolean isMultiInsert(MySqlInsertStatement insertStmt) {
    return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1)
        || insertStmt.getQuery() != null;
  }

  /**
   * 单条insert（非批量）
   *
   * @param schema
   * @param rrs
   * @param partitionColumn
   * @param tableName
   * @param insertStmt
   * @throws SQLNonTransientException
   */
  private void parserSingleInsert(SchemaConfig schema, RouteResultset rrs, String partitionColumn,
      String tableName,
      MySqlInsertStatement insertStmt) throws SQLNonTransientException {
    boolean isFound = false;
    for (int i = 0; i < insertStmt.getColumns().size(); i++) {
      if (partitionColumn
          .equalsIgnoreCase(
              StringUtil.removeBackquote(insertStmt.getColumns().get(i).toString()))) {// 找到分片字段
        isFound = true;
        String column = StringUtil.removeBackquote(insertStmt.getColumns().get(i).toString());

        String value = StringUtil
            .removeBackquote(insertStmt.getValues().getValues().get(i).toString());

        RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
        routeCalculateUnit.addShardingExpr(tableName, column, value);
        ctx.addRouteCalculateUnit(routeCalculateUnit);
        // 是单分片键，找到了就返回
        break;
      }
    }
    if (!isFound) {// 分片表的
      String msg =
          "bad insert sql (sharding column:" + partitionColumn + " not provided," + insertStmt;
      LOGGER.warn(msg);
      throw new SQLNonTransientException(msg);
    }
    // insert into .... on duplicateKey
    // such as :INSERT INTO TABLEName (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE
    // b=VALUES(b);
    // INSERT INTO TABLEName (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE c=c+1;
    if (insertStmt.getDuplicateKeyUpdate() != null) {
      List<SQLExpr> updateList = insertStmt.getDuplicateKeyUpdate();
      for (SQLExpr expr : updateList) {
        SQLBinaryOpExpr opExpr = (SQLBinaryOpExpr) expr;
        String column = StringUtil.removeBackquote(opExpr.getLeft().toString().toUpperCase());
        if (column.equals(partitionColumn)) {
          String msg = "Sharding column can't be updated: " + tableName + " -> " + partitionColumn;
          LOGGER.warn(msg);
          throw new SQLNonTransientException(msg);
        }
      }
    }
  }

  /**
   * insert into .... select .... 或insert into table() values (),(),....
   *
   * @param schema
   * @param rrs
   * @param insertStmt
   * @throws SQLNonTransientException
   */
  private void parserBatchInsert(SchemaConfig schema, RouteResultset rrs, String partitionColumn,
      String tableName,
      MySqlInsertStatement insertStmt) throws SQLNonTransientException {
    // insert into table() values (),(),....
    if (insertStmt.getValuesList().size() > 1) {
      // 字段列数
      int columnNum = insertStmt.getColumns().size();
      int shardingColIndex = getSharingColIndex(insertStmt, partitionColumn);
      if (shardingColIndex == -1) {
        String msg =
            "bad insert sql (sharding column:" + partitionColumn + " not provided," + insertStmt;
        LOGGER.warn(msg);
        throw new SQLNonTransientException(msg);
      } else {
        List<ValuesClause> valueClauseList = insertStmt.getValuesList();

        Map<String, List<ValuesClause>> nodeValuesMap = new HashMap<String, List<ValuesClause>>();
        TableConfig tableConfig = schema.getTables().get(tableName);
        RuleAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();

        for (ValuesClause valueClause : valueClauseList) {
          if (valueClause.getValues().size() != columnNum) {
            String msg = "bad insert sql columnSize != valueSize:" + columnNum + " != "
                + valueClause.getValues().size() + "values:" + valueClause;
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
          }
          SQLExpr expr = valueClause.getValues().get(shardingColIndex);
          String shardingValue = null;
          if (expr instanceof SQLIntegerExpr) {
            SQLIntegerExpr intExpr = (SQLIntegerExpr) expr;
            shardingValue = intExpr.getNumber() + "";
          } else if (expr instanceof SQLCharExpr) {
            SQLCharExpr charExpr = (SQLCharExpr) expr;
            shardingValue = charExpr.getText();
          }

          // zichun.niu批量插入
          // Integer nodeIndex = algorithm.calculate(shardingValue);
          Integer tableIndex = algorithm.calculateTable(shardingValue);
          Integer nodeIndex = algorithm.calculateForDBIndex(tableIndex);
          if (nodeIndex == null) {
            String msg =
                "can't find any valid datanode :" + tableName + " -> " + partitionColumn + " -> "
                    + shardingValue;
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
          }

          String nodeValue = new StringBuilder().append(tableConfig.getDataNodes().get(nodeIndex))
              .append(DmdsConstants.seq_db_tb)
              .append(TbSeqUtil.getDivTableName(tableName, tableIndex))
              .toString();

          if (nodeValuesMap.get(nodeValue) == null) {
            nodeValuesMap.put(nodeValue, new ArrayList<ValuesClause>());
          }
          nodeValuesMap.get(nodeValue).add(valueClause);
        }

        RouteResultsetNode[] nodes = new RouteResultsetNode[nodeValuesMap.size()];
        int count = 0;
        for (Map.Entry<String, List<ValuesClause>> node : nodeValuesMap.entrySet()) {
          String nodeValue = node.getKey();
          List<ValuesClause> valuesList = node.getValue();
          insertStmt.setValuesList(valuesList);
          nodes[count++] = new RouteResultsetNode(nodeValue, rrs.getSqlType(),
              insertStmt.toString());
        }
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
      }
    } else if (insertStmt.getQuery() != null) { // insert into .... select ....
      String msg = "TODO:insert into .... select .... not supported!";
      LOGGER.warn(msg);
      throw new SQLNonTransientException(msg);
    }
  }

  /**
   * 寻找拆分字段在 columnList中的索引
   *
   * @param insertStmt
   * @param partitionColumn
   * @return
   */
  private int getSharingColIndex(MySqlInsertStatement insertStmt, String partitionColumn) {
    int shardingColIndex = -1;
    for (int i = 0; i < insertStmt.getColumns().size(); i++) {
      if (partitionColumn
          .equalsIgnoreCase(
              StringUtil.removeBackquote(insertStmt.getColumns().get(i).toString()))) {// 找到分片字段
        shardingColIndex = i;
        return shardingColIndex;
      }
    }
    return shardingColIndex;
  }
}
