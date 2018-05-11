/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.wall.spi.WallVisitorUtils;
import com.zhongan.dmds.cache.LayerCachePool;
import com.zhongan.dmds.commons.contants.mysql.DmdsConstants;
import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.commons.util.TableDivNode;
import com.zhongan.dmds.commons.util.TbSeqUtil;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.TableConfig;
import com.zhongan.dmds.config.model.rule.RuleAlgorithm;
import com.zhongan.dmds.config.model.rule.RuleConfig;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.exception.SqlParseException;
import com.zhongan.dmds.mpp.ColumnRoutePair;
import com.zhongan.dmds.mpp.LoadData;
import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.route.RouteResultsetNode;
import com.zhongan.dmds.sqlParser.druid.DruidShardingParseInfo;
import com.zhongan.dmds.sqlParser.druid.RouteCalculateUnit;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.*;

//import com.zhongan.dmds.commons.util.TableDivNodeUtil;

/**
 * 从RouterUtil拆分部分方法
 * 2016.12 dmds支持分表功能
 */
public class DmdsRouterUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(DmdsRouterUtil.class);

  /**
   * 移除执行语句中的数据库名
   *
   * @param stmt   执行语句
   * @param schema 数据库名
   * @return 执行语句
   */
  public static String removeSchema(String stmt, String schema) {
    final String upStmt = stmt.toUpperCase();
    final String upSchema = schema.toUpperCase() + ".";
    int strtPos = 0;
    int indx = 0;
    boolean flag = false;
    indx = upStmt.indexOf(upSchema, strtPos);
    if (indx < 0) {
      StringBuilder sb = new StringBuilder("`").append(schema.toUpperCase()).append("`.");
      indx = upStmt.indexOf(sb.toString(), strtPos);
      flag = true;
      if (indx < 0) {
        return stmt;
      }
    }
    StringBuilder sb = new StringBuilder();
    int firstE = upStmt.indexOf("'");
    int endE = upStmt.lastIndexOf("'");
    while (indx > 0) {
      sb.append(stmt.substring(strtPos, indx));
      strtPos = indx + upSchema.length();
      if (flag) {
        strtPos += 2;
      }
      if (indx > firstE && indx < endE && countChar(stmt, indx) % 2 == 1) {
        sb.append(stmt.substring(indx, indx + schema.length() + 1));
      }
      indx = upStmt.indexOf(upSchema, strtPos);
    }
    sb.append(stmt.substring(strtPos));
    return sb.toString();
  }

  private static int countChar(String sql, int end) {
    int count = 0;
    for (int i = 0; i < end; i++) {
      if (sql.charAt(i) == '\'') {
        count++;
      }
    }
    return count;
  }

  /**
   * 获取第一个节点作为路由
   *
   * @param rrs      数据路由集合
   * @param dataNode 数据库所在节点
   * @param stmt     执行语句
   * @return 数据路由集合
   */
  public static RouteResultset routeToSingleNode(RouteResultset rrs, String dataNode, String stmt) {
    if (dataNode == null) {
      return rrs;
    }
    RouteResultsetNode[] nodes = new RouteResultsetNode[1];
    nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), stmt);// rrs.getStatement()
    rrs.setNodes(nodes);
    rrs.setFinishedRoute(true);
    if (rrs.getCanRunInReadDB() != null) {
      nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
    }
    return rrs;
  }

  /**
   * 修复DDL路由
   *
   * @return RouteResultset
   * @author aStoneGod
   */
  public static RouteResultset routeToDDLNode(RouteResultset rrs, int sqlType, String stmt,
      SchemaConfig schema)
      throws SQLSyntaxErrorException {
    // 检查表是否在配置文件中
    stmt = getFixedSql(stmt);
    String tablename = "";
    final String upStmt = stmt.toUpperCase();
    if (upStmt.startsWith("CREATE")) {
      if (upStmt.contains("CREATE INDEX ")) {
        tablename = DmdsRouterUtil.getTableName(stmt, DmdsRouterUtil.getCreateIndexPos(upStmt, 0));
      } else {
        tablename = DmdsRouterUtil.getTableName(stmt, DmdsRouterUtil.getCreateTablePos(upStmt, 0));
      }
    } else if (upStmt.startsWith("DROP")) {
      if (upStmt.contains("DROP INDEX ")) {
        tablename = DmdsRouterUtil.getTableName(stmt, DmdsRouterUtil.getDropIndexPos(upStmt, 0));
      } else {
        tablename = DmdsRouterUtil.getTableName(stmt, DmdsRouterUtil.getDropTablePos(upStmt, 0));
      }
    } else if (upStmt.startsWith("ALTER")) {
      tablename = DmdsRouterUtil.getTableName(stmt, DmdsRouterUtil.getAlterTablePos(upStmt, 0));

    } else if (upStmt.startsWith("TRUNCATE")) {
      tablename = DmdsRouterUtil.getTableName(stmt, DmdsRouterUtil.getTruncateTablePos(upStmt, 0));
    }
    tablename = tablename.toUpperCase();

    if (schema.getTables().containsKey(tablename)) {
      if (ServerParse.DDL == sqlType) {
        // 如果是dll语句的时候主动走全部语句
        if (schema.getTables() != null && schema.getTables().get(tablename) != null) {
          TableConfig tableConfig = schema.getTables().get(tablename);
          RuleAlgorithm algorithm = null;
          if (tableConfig.getRule() != null) {
            algorithm = tableConfig.getRule().getRuleAlgorithm();
          }
          Set<String> fnodes = getFullDataNodes(tablename, algorithm, tableConfig.getDataNodes());
          RouteResultsetNode[] nodes = new RouteResultsetNode[fnodes.size()];
          Iterator<String> iter = fnodes.iterator();
          for (int i = 0; i < fnodes.size(); i++) {
            String name = iter.next();
            nodes[i] = new RouteResultsetNode(name, sqlType, stmt);
          }
          rrs.setNodes(nodes);
        }
      }
      return rrs;
    } else if (schema.getDataNode() != null) { // 默认节点ddl
      RouteResultsetNode[] nodes = new RouteResultsetNode[1];
      nodes[0] = new RouteResultsetNode(schema.getDataNode(), sqlType, stmt);
      rrs.setNodes(nodes);
      return rrs;
    }
    // both tablename and defaultnode null
    LOGGER.error("table not in schema----" + tablename);
    throw new SQLSyntaxErrorException("op table not in schema----" + tablename);
  }

  /**
   * 处理SQL
   *
   * @param stmt 执行语句
   * @return 处理后SQL
   * @author AStoneGod
   */
  public static String getFixedSql(String stmt) {
    stmt = stmt.replaceAll("\r\n", " "); // 对于\r\n的字符 用 空格处理
    stmt = stmt.replaceAll("\n\t", " "); // 对于\n\t的字符 用 空格处理
    // stmt = stmt.replaceAll("`", "");
    return stmt = stmt.trim(); // .toUpperCase();
  }

  /**
   * 获取table名字
   *
   * @param stmt   执行语句
   * @param repPos 开始位置和位数
   * @return 表名
   * @author AStoneGod
   */
  public static String getTableName(String stmt, int[] repPos) {
    int startPos = repPos[0];
    int secInd = stmt.indexOf(' ', startPos + 1);
    if (secInd < 0) {
      secInd = stmt.length();
    }
    int thiInd = stmt.indexOf('(', secInd + 1);
    if (thiInd < 0) {
      thiInd = stmt.length();
    }
    repPos[1] = secInd;
    String tableName = "";
    if (stmt.toUpperCase().startsWith("DESC") || stmt.toUpperCase().startsWith("DESCRIBE")) {
      tableName = stmt.substring(startPos, thiInd).trim();
    } else {
      tableName = stmt.substring(secInd, thiInd).trim();
    }

    // ALTER TABLE
    if (tableName.contains(" ")) {
      tableName = tableName.substring(0, tableName.indexOf(" "));
    }
    int ind2 = tableName.indexOf('.');
    if (ind2 > 0) {
      tableName = tableName.substring(ind2 + 1);
    }
    return tableName;
  }

  /**
   * 获取show语句table名字
   *
   * @param stmt   执行语句
   * @param repPos 开始位置和位数
   * @return 表名
   * @author AStoneGod
   */
  public static String getShowTableName(String stmt, int[] repPos) {
    int startPos = repPos[0];
    int secInd = stmt.indexOf(' ', startPos + 1);
    if (secInd < 0) {
      secInd = stmt.length();
    }

    repPos[1] = secInd;
    String tableName = stmt.substring(startPos, secInd).trim();

    int ind2 = tableName.indexOf('.');
    if (ind2 > 0) {
      tableName = tableName.substring(ind2 + 1);
    }
    return tableName;
  }

  /**
   * 获取语句中前关键字位置和占位个数表名位置
   *
   * @param upStmt 执行语句
   * @param start  开始位置
   * @return int[] 关键字位置和占位个数
   * <p>
   * TODO fixed it
   */
  public static int[] getCreateTablePos(String upStmt, int start) {
    String token1 = "CREATE ";
    String token2 = " TABLE ";
    int createInd = upStmt.indexOf(token1, start);
    int tabInd = upStmt.indexOf(token2, start);
    // 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
    if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
      return new int[]{tabInd, token2.length()};
    } else {
      return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
    }
  }

  /**
   * 获取语句中前关键字位置和占位个数表名位置
   *
   * @param upStmt 执行语句
   * @param start  开始位置
   * @return int[]关键字位置和占位个数
   * @author aStoneGod
   */
  public static int[] getCreateIndexPos(String upStmt, int start) {
    String token1 = "CREATE ";
    String token2 = " INDEX ";
    String token3 = " ON ";
    int createInd = upStmt.indexOf(token1, start);
    int idxInd = upStmt.indexOf(token2, start);
    int onInd = upStmt.indexOf(token3, start);
    // 既包含CREATE又包含INDEX，且CREATE关键字在INDEX关键字之前, 且包含ON...
    if (createInd >= 0 && idxInd > 0 && idxInd > createInd && onInd > 0 && onInd > idxInd) {
      return new int[]{onInd, token3.length()};
    } else {
      return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
    }
  }

  /**
   * 获取ALTER语句中前关键字位置和占位个数表名位置
   *
   * @param upStmt 执行语句
   * @param start  开始位置
   * @return int[] 关键字位置和占位个数
   * @author aStoneGod
   */
  public static int[] getAlterTablePos(String upStmt, int start) {
    String token1 = "ALTER ";
    String token2 = " TABLE ";
    int createInd = upStmt.indexOf(token1, start);
    int tabInd = upStmt.indexOf(token2, start);
    // 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
    if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
      return new int[]{tabInd, token2.length()};
    } else {
      return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
    }
  }

  /**
   * 获取DROP语句中前关键字位置和占位个数表名位置
   *
   * @param upStmt 执行语句
   * @param start  开始位置
   * @return int[] 关键字位置和占位个数
   * @author aStoneGod
   */
  public static int[] getDropTablePos(String upStmt, int start) {
    // 增加 if exists判断
    if (upStmt.contains("EXISTS")) {
      String token1 = "IF ";
      String token2 = " EXISTS ";
      int ifInd = upStmt.indexOf(token1, start);
      int tabInd = upStmt.indexOf(token2, start);
      if (ifInd >= 0 && tabInd > 0 && tabInd > ifInd) {
        return new int[]{tabInd, token2.length()};
      } else {
        return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
      }
    } else {
      String token1 = "DROP ";
      String token2 = " TABLE ";
      int createInd = upStmt.indexOf(token1, start);
      int tabInd = upStmt.indexOf(token2, start);

      if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
        return new int[]{tabInd, token2.length()};
      } else {
        return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
      }
    }
  }

  /**
   * 获取DROP语句中前关键字位置和占位个数表名位置
   *
   * @param upStmt 执行语句
   * @param start  开始位置
   * @return int[]关键字位置和占位个数
   * @author aStoneGod
   */

  public static int[] getDropIndexPos(String upStmt, int start) {
    String token1 = "DROP ";
    String token2 = " INDEX ";
    String token3 = " ON ";
    int createInd = upStmt.indexOf(token1, start);
    int idxInd = upStmt.indexOf(token2, start);
    int onInd = upStmt.indexOf(token3, start);
    // 既包含CREATE又包含INDEX，且CREATE关键字在INDEX关键字之前, 且包含ON...
    if (createInd >= 0 && idxInd > 0 && idxInd > createInd && onInd > 0 && onInd > idxInd) {
      return new int[]{onInd, token3.length()};
    } else {
      return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
    }
  }

  /**
   * 获取TRUNCATE语句中前关键字位置和占位个数表名位置
   *
   * @param upStmt 执行语句
   * @param start  开始位置
   * @return int[] 关键字位置和占位个数
   * @author aStoneGod
   */
  public static int[] getTruncateTablePos(String upStmt, int start) {
    String token1 = "TRUNCATE ";
    String token2 = " TABLE ";
    int createInd = upStmt.indexOf(token1, start);
    int tabInd = upStmt.indexOf(token2, start);
    // 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
    if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
      return new int[]{tabInd, token2.length()};
    } else {
      return new int[]{-1, token2.length()};// 不满足条件时，只关注第一个返回值为-1，第二个任意
    }
  }

  /**
   * 获取语句中前关键字位置和占位个数表名位置
   *
   * @param upStmt 执行语句
   * @param start  开始位置
   * @return int[] 关键字位置和占位个数
   */
  public static int[] getSpecPos(String upStmt, int start) {
    String token1 = " FROM ";
    String token2 = " IN ";
    int tabInd1 = upStmt.indexOf(token1, start);
    int tabInd2 = upStmt.indexOf(token2, start);
    if (tabInd1 > 0) {
      if (tabInd2 < 0) {
        return new int[]{tabInd1, token1.length()};
      }
      return (tabInd1 < tabInd2) ? new int[]{tabInd1, token1.length()}
          : new int[]{tabInd2, token2.length()};
    } else {
      return new int[]{tabInd2, token2.length()};
    }
  }

  /**
   * 获取开始位置后的 LIKE、WHERE 位置 如果不含 LIKE、WHERE 则返回执行语句的长度
   *
   * @param upStmt 执行sql
   * @param start  开发位置
   * @return int
   */
  public static int getSpecEndPos(String upStmt, int start) {
    int tabInd = upStmt.toUpperCase().indexOf(" LIKE ", start);
    if (tabInd < 0) {
      tabInd = upStmt.toUpperCase().indexOf(" WHERE ", start);
    }
    if (tabInd < 0) {
      return upStmt.length();
    }
    return tabInd;
  }

  public static RouteResultset routeToMultiNode(boolean cache, RouteResultset rrs,
      Collection<String> dataNodes,
      String stmt, String tableName) {
    RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
    int i = 0;
    RouteResultsetNode node;
    for (String dataNode : dataNodes) {

      node = new RouteResultsetNode(dataNode, rrs.getSqlType(), stmt);
      if (rrs.getCanRunInReadDB() != null) {
        node.setCanRunInReadDB(rrs.getCanRunInReadDB());
      }
      nodes[i++] = node;

    }
    rrs.setCacheAble(cache);
    rrs.setNodes(nodes);

    return rrs;
  }

  public static RouteResultset routeToMultiNode(boolean cache, RouteResultset rrs,
      Collection<String> dataNodes,
      String stmt, boolean isGlobalTable) {

    rrs = routeToMultiNode(cache, rrs, dataNodes, stmt, null);
    rrs.setGlobalTable(isGlobalTable);
    return rrs;
  }

  public static void routeForTableMeta(RouteResultset rrs, SchemaConfig schema, String tableName,
      String sql) {
    String dataNode = null;
    if (isNoSharding(schema, tableName)) {// 不分库的直接从schema中获取dataNode
      dataNode = schema.getDataNode();
    } else {
      dataNode = getMetaReadDataNode(schema, tableName);
    }

    RouteResultsetNode[] nodes = new RouteResultsetNode[1];
    nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), sql);
    if (rrs.getCanRunInReadDB() != null) {
      nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
    }
    rrs.setNodes(nodes);
  }

  /**
   * 根据标名随机获取一个节点
   *
   * @param schema 数据库名
   * @param table  表名
   * @return 数据节点
   */
  private static String getMetaReadDataNode(SchemaConfig schema, String table) {
    // Table名字被转化为大写的，存储在schema
    table = table.toUpperCase();
    String dataNode = null;
    Map<String, TableConfig> tables = schema.getTables();
    TableConfig tc;
    if (tables != null && (tc = tables.get(table)) != null) {
      // dataNode = tc.getRandomDataNode();
      dataNode = tc.getDataNodes().get(0) + "#" + table + "_0000";
    }
    return dataNode;
  }

  /**
   * @return dataNodeIndex -&gt; [partitionKeysValueTuple+]
   */
  public static Set<String> ruleByJoinValueCalculate(RouteResultset rrs, TableConfig tc,
      Set<ColumnRoutePair> colRoutePairSet) throws SQLNonTransientException {

    String joinValue = "";
    if (colRoutePairSet.size() > 1) {
      LOGGER.warn("joinKey can't have multi Value");
    } else {
      Iterator<ColumnRoutePair> it = colRoutePairSet.iterator();
      ColumnRoutePair joinCol = it.next();
      joinValue = joinCol.colValue;
      LOGGER.info("joinValue : " + joinValue);
    }

    Set<String> retNodeSet = new LinkedHashSet<String>();

    Set<String> nodeSet = new LinkedHashSet<String>();
    if (tc.isSecondLevel() && tc.getParentTC().getPartitionColumn()
        .equals(tc.getParentKey())) { // using
      // parent
      // rule
      // to
      // find
      // datanode

      nodeSet = ruleCalculate(tc.getParentTC(), colRoutePairSet);
      if (nodeSet.isEmpty()) {
        throw new SQLNonTransientException(
            "parent key can't find  valid datanode ,expect 1 but found: " + nodeSet.size());
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "found partion node (using parent partion rule directly) for child table to insert  "
                + nodeSet + " sql :" + rrs.getStatement());
      }
      retNodeSet.addAll(nodeSet);

      return retNodeSet;
    } else {
      retNodeSet.addAll(tc.getParentTC().getDataNodes());
    }

    return retNodeSet;
  }

  /**
   * @return dataNodeIndex -&gt; [partitionKeysValueTuple+]
   */
  public static Set<String> ruleCalculate(TableConfig tc, Set<ColumnRoutePair> colRoutePairSet) {
    Set<String> routeNodeSet = new LinkedHashSet<String>();
    String col = tc.getRule().getColumn();
    RuleConfig rule = tc.getRule();
    RuleAlgorithm algorithm = rule.getRuleAlgorithm();
    for (ColumnRoutePair colPair : colRoutePairSet) {
      if (colPair.colValue != null) {
        Integer tableIndex = algorithm.calculateTable(colPair.colValue);
        Integer nodeIndx = algorithm.calculateForDBIndex(tableIndex);
        if (nodeIndx == null) {
          throw new IllegalArgumentException(
              "can't find datanode for sharding column:" + col + " val:" + colPair.colValue);
        } else {
          String dataNode = tc.getDataNodes().get(nodeIndx);
          routeNodeSet.add(dataNode);
          colPair.setNodeId(nodeIndx);
        }
      } else if (colPair.rangeValue != null) {
        Integer[] nodeRange = algorithm
            .calculateRange(String.valueOf(colPair.rangeValue.beginValue),
                String.valueOf(colPair.rangeValue.endValue));
        if (nodeRange != null) {
          /**
           * 不能确认 colPair的 nodeid是否会有其它影响
           */
          if (nodeRange.length == 0) {
            routeNodeSet.addAll(tc.getDataNodes());
          } else {
            ArrayList<String> dataNodes = tc.getDataNodes();
            String dataNode = null;
            for (Integer nodeId : nodeRange) {
              dataNode = dataNodes.get(nodeId);
              routeNodeSet.add(dataNode);
            }
          }
        }
      }
    }
    return routeNodeSet;
  }

  /**
   * 多表路由
   */
  public static RouteResultset tryRouteForTables(SchemaConfig schema, DruidShardingParseInfo ctx,
      RouteCalculateUnit routeUnit, RouteResultset rrs, boolean isSelect, LayerCachePool cachePool)
      throws SQLNonTransientException {
    List<String> tables = ctx.getTables();
    if (schema.isNoSharding() || (tables.size() >= 1 && isNoSharding(schema, tables.get(0)))) {
      return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
    }

    // 只有一个表的
    if (tables.size() == 1) {
      return DmdsRouterUtil
          .tryRouteForOneTable(schema, ctx, routeUnit, tables.get(0), rrs, isSelect, cachePool);
    }

    Set<String> retNodesSet = new HashSet<String>();
    // 每个表对应的路由映射
    Map<String, Set<String>> tablesRouteMap = new HashMap<String, Set<String>>();

    // 分库解析信息不为空
    Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = routeUnit
        .getTablesAndConditions();
    if (tablesAndConditions != null && tablesAndConditions.size() > 0) {
      // 为分库表找路由
      DmdsRouterUtil
          .findRouteWithcConditionsForTables(schema, rrs, tablesAndConditions, tablesRouteMap,
              ctx.getSql(), cachePool, isSelect);
      if (rrs.isFinishedRoute()) {
        return rrs;
      }
    } else {
      // guoweidong 为没有条件查询的联表sql路由
      for (String tableName : tables) {
        TableConfig tableConfig = schema.getTables().get(tableName.toUpperCase());
        buildAllNodeRoute(tablesRouteMap, tableConfig, tableName);
      }

    }

    // 为全局表和单库表找路由
    for (String tableName : tables) {
      TableConfig tableConfig = schema.getTables().get(tableName.toUpperCase());
      if (tableConfig == null) {
        String msg =
            "can't find table define in schema " + tableName + " schema:" + schema.getName();
        LOGGER.warn(msg);
        throw new SQLNonTransientException(msg);
      }
      if (tableConfig.isGlobalTable()) {// 全局表
        if (tablesRouteMap.get(tableName) == null) {
          tablesRouteMap.put(tableName, new HashSet<String>());
        }
        tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
      } else if (tablesRouteMap.get(tableName) == null) { // 余下的表都是单库表

        tablesRouteMap.put(tableName, new HashSet<String>());
        // guoweidong修改
        // tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
        buildAllNodeRoute(tablesRouteMap, tableConfig, tableName);
      }
    }

    boolean isFirstAdd = true;
    List<TableDivNode> tableDivNodeList = new ArrayList<TableDivNode>();
    for (Map.Entry<String, Set<String>> entry : tablesRouteMap.entrySet()) {
      if (entry.getValue() == null || entry.getValue().size() == 0) {
        throw new SQLNonTransientException("parent key can't find any valid datanode ");
      } else {
        if (isFirstAdd) {
          tableDivNodeList = TableDivNode.getNodeList(entry.getValue());
          isFirstAdd = false;
        } else {
          tableDivNodeList = TableDivNode.addNodeSet(tableDivNodeList, entry.getValue());
        }
      }
    }

    if (tableDivNodeList.size() == 0) {
      String errMsg =
          "invalid route in sql, multi tables found but datanode has no intersection " + " sql:"
              + ctx.getSql();
      LOGGER.warn(errMsg);
      throw new SQLNonTransientException(errMsg);
    } else {
      retNodesSet = TableDivNode.toNodeSet(tableDivNodeList);
    }

    if (retNodesSet != null && retNodesSet.size() > 0) {
      if (retNodesSet.size() > 1 && isAllGlobalTable(ctx, schema)) {
        // mulit routes ,not cache route result
        if (isSelect) {
          rrs.setCacheAble(false);
          routeToSingleNode(rrs, retNodesSet.iterator().next(), ctx.getSql());
        } else {// delete 删除全局表的记录
          routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql(), true);
        }
      } else {
        routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql(), null);
      }
    }
    return rrs;
  }

  /**
   * 单表路由
   */
  public static RouteResultset tryRouteForOneTable(SchemaConfig schema, DruidShardingParseInfo ctx,
      RouteCalculateUnit routeUnit, String tableName, RouteResultset rrs, boolean isSelect,
      LayerCachePool cachePool) throws SQLNonTransientException {

    if (isNoSharding(schema, tableName)) {
      return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
    }

    TableConfig tc = schema.getTables().get(tableName);
    if (tc == null) {
      String msg = "can't find table define in schema " + tableName + " schema:" + schema.getName();
      LOGGER.warn(msg);
      throw new SQLNonTransientException(msg);
    }

    if (tc.isGlobalTable()) {// 全局表
      if (isSelect) {
        // global select ,not cache route result
        rrs.setCacheAble(false);
        return routeToSingleNode(rrs, tc.getRandomDataNode(), ctx.getSql());
      } else {// insert into 全局表的记录
        return routeToMultiNode(false, rrs, tc.getDataNodes(), ctx.getSql(), true);
      }
    } else {// 单表或者分库表
      if (!checkRuleRequired(schema, ctx, routeUnit, tc)) {
        throw new IllegalArgumentException(
            "route rule for table " + tc.getName() + " is required: " + ctx.getSql());
      }
      // 不分库分表的
      if (tc.getPartitionColumn() == null && !tc.isSecondLevel()) {// 单表且不是childTable
        return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes(), ctx.getSql(), null);
      } else {
        // 每个表对应的路由映射
        Map<String, Set<String>> tablesRouteMap = new HashMap<String, Set<String>>();
        if (routeUnit.getTablesAndConditions() != null
            && routeUnit.getTablesAndConditions().size() > 0) {
          DmdsRouterUtil
              .findRouteWithcConditionsForTables(schema, rrs, routeUnit.getTablesAndConditions(),
                  tablesRouteMap, ctx.getSql(), cachePool, isSelect);
          if (rrs.isFinishedRoute()) {
            return rrs;
          }
        }

        if (tablesRouteMap.get(tableName) == null) {
          // niuzichun 没有计算出分表的全部扫描
          buildAllNodeRoute(tablesRouteMap, tc, tableName);

          if (tablesRouteMap.get(tableName) == null) {
            return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes(), ctx.getSql(),
                tableName);
          } else {
            return routeToMultiNode(rrs.isCacheAble(), rrs, tablesRouteMap.get(tableName),
                ctx.getSql(),
                tableName);
          }
        } else {
          return routeToMultiNode(rrs.isCacheAble(), rrs, tablesRouteMap.get(tableName),
              ctx.getSql(),
              tableName);
        }
      }
    }
  }

  private static void buildAllNodeRoute(Map<String, Set<String>> tablesRouteMap,
      TableConfig tableConfig,
      String tableName) {
    if (tablesRouteMap.get(tableName) == null) {
      tablesRouteMap.put(tableName, new HashSet<String>());
    }

    if (tableConfig == null || tableConfig.getRule() == null
        || tableConfig.getRule().getRuleAlgorithm() == null) {
      return;
    }

    Set<String> nodes = DmdsRouterUtil
        .getFullDataNodes(tableName, tableConfig.getRule().getRuleAlgorithm(),
            tableConfig.getDataNodes());
    tablesRouteMap.get(tableName).addAll(nodes);
  }

  /**
   * 是否单库单表的
   *
   * @return
   */
  private static boolean isSignalTable(SchemaConfig schema, String tableName) {
    TableConfig tableConfig = schema.getTables().get(tableName);

    Integer tbCount = 1;
    if (tableConfig.getRule() != null && tableConfig.getRule().getRuleAlgorithm() != null) {
      tbCount = tableConfig.getRule().getRuleAlgorithm().getTbCount();
    }

    if ((tbCount == null || tbCount == 1) && tableConfig.getDataNodes().size() == 1) {
      return true;
    }

    return false;
  }

  /**
   * 处理分库表路由
   */
  public static void findRouteWithcConditionsForTables(SchemaConfig schema, RouteResultset rrs,
      Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions,
      Map<String, Set<String>> tablesRouteMap,
      String sql, LayerCachePool cachePool, boolean isSelect) throws SQLNonTransientException {

    // 为分库表找路由
    for (Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions
        .entrySet()) {
      String tableName = entry.getKey().toUpperCase();
      TableConfig tableConfig = schema.getTables().get(tableName);
      if (tableConfig == null) {
        String msg =
            "can't find table define in schema " + tableName + " schema:" + schema.getName();
        LOGGER.warn(msg);
        throw new SQLNonTransientException(msg);
      }
      // 全局表或者不分库的表略过（全局表后面再计算）
      // niuzichun 单库多表的时候会走扫描所有表
      if (tableConfig.isGlobalTable() || isSignalTable(schema, tableName)) {
        continue;
      } else {
        // 非全局表：分库表、childTable、其他
        Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
        String joinKey = tableConfig.getJoinKey();
        String partionCol = tableConfig.getPartitionColumn();
        String primaryKey = tableConfig.getPrimaryKey();
        boolean isFoundPartitionValue =
            partionCol != null && entry.getValue().get(partionCol) != null;
        boolean isLoadData = false;
        if (LOGGER.isDebugEnabled()) {
          if (sql.startsWith(LoadData.loadDataHint) || rrs.isLoadData()) { // 由于load
            // data一次会计算很多路由数据，如果输出此日志会极大降低load
            // data的性能
            isLoadData = true;
          }
        }
        if (entry.getValue().get(primaryKey) != null && entry.getValue().size() == 1
            && !isLoadData) {// 主键查找
          // try by primary key if found in cache
          Set<ColumnRoutePair> primaryKeyPairs = entry.getValue().get(primaryKey);
          if (primaryKeyPairs != null) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("try to find cache by primary key ");
            }
            String tableKey = schema.getName() + '_' + tableName;
            boolean allFound = true;
            for (ColumnRoutePair pair : primaryKeyPairs) {// 可能id
              // in(1,2,3)多主键
              String cacheKey = pair.colValue;
              String dataNode = (String) cachePool.get(tableKey, cacheKey);
              if (dataNode == null) {
                allFound = false;
                continue;
              } else {
                if (tablesRouteMap.get(tableName) == null) {
                  tablesRouteMap.put(tableName, new HashSet<String>());
                }
                tablesRouteMap.get(tableName).add(dataNode);
                continue;
              }
            }
            if (!allFound) {
              // need cache primary key ->datanode relation
              if (isSelect && tableConfig.getPrimaryKey() != null) {
                rrs.setPrimaryKey(tableKey + '.' + tableConfig.getPrimaryKey());
              }
            } else {// 主键缓存中找到了就执行循环的下一轮
              continue;
            }
          }
        }
        if (isFoundPartitionValue) {// 分库表
          Set<ColumnRoutePair> partitionValue = columnsMap.get(partionCol);
          if (partitionValue == null || partitionValue.size() == 0) {
            if (tablesRouteMap.get(tableName) == null) {
              tablesRouteMap.put(tableName, new HashSet<String>());
            }
            tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
          } else {
            for (ColumnRoutePair pair : partitionValue) {
              if (pair.colValue != null) {
                // 计算分表
                Integer tableIndex = tableConfig.getRule().getRuleAlgorithm()
                    .calculateTable(pair.colValue);

                // 计算数据节点 计算分库
                Integer nodeIndex = tableConfig.getRule().getRuleAlgorithm()
                    .calculateForDBIndex(tableIndex);

                if (nodeIndex == null) {
                  String msg = "can't find any valid datanode :" + tableConfig.getName() + " -> "
                      + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
                  LOGGER.warn(msg);
                  throw new SQLNonTransientException(msg);
                }

                ArrayList<String> dataNodes = tableConfig.getDataNodes();
                String node;
                if (nodeIndex >= 0 && nodeIndex < dataNodes.size()) {
                  node = dataNodes.get(nodeIndex);
                } else {
                  node = null;
                  String msg = "Can't find a valid data node for specified node index :"
                      + tableConfig.getName() + " -> " + tableConfig.getPartitionColumn() + " -> "
                      + pair.colValue + " -> " + "Index : " + nodeIndex;
                  LOGGER.warn(msg);
                  throw new SQLNonTransientException(msg);
                }

                // 保存到路由信息中
                if (node != null) {
                  if (tablesRouteMap.get(tableName) == null) {
                    tablesRouteMap.put(tableName, new HashSet<String>());
                  }
                  StringBuilder nodeValue = new StringBuilder().append(node);
                  if (tableIndex != null) {
                    nodeValue.append(DmdsConstants.seq_db_tb)
                        .append(TbSeqUtil.getDivTableName(tableName, tableIndex));
                  }
                  tablesRouteMap.get(tableName).add(nodeValue.toString());
                }

              }

              // 处理bettween 的
              if (pair.rangeValue != null) {
                // 获取所有符合条件的数据节点
                Integer[] nodeIndexs = tableConfig.getRule().getRuleAlgorithm().calculateRange(
                    pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
                ArrayList<String> dataNodes = tableConfig.getDataNodes();
                String node;
                for (Integer idx : nodeIndexs) {
                  if (idx >= 0 && idx < dataNodes.size()) {
                    node = dataNodes.get(idx);
                  } else {
                    String msg =
                        "Can't find valid data node(s) for some of specified node indexes :"
                            + tableConfig.getName() + " -> " + tableConfig.getPartitionColumn();
                    LOGGER.warn(msg);
                    throw new SQLNonTransientException(msg);
                  }
                  if (node != null) {
                    if (tablesRouteMap.get(tableName) == null) {
                      tablesRouteMap.put(tableName, new HashSet<String>());
                    }
                    tablesRouteMap.get(tableName).add(node);
                  }
                }
              }
            }
          }
        } else if (joinKey != null && columnsMap.get(joinKey) != null
            && columnsMap.get(joinKey).size() != 0) {// (如果是select
          // 语句的父子表join)之前要找到root
          // table,只留下root
          // table
          Set<ColumnRoutePair> joinKeyValue = columnsMap.get(joinKey);

          Set<String> dataNodeSet = ruleByJoinValueCalculate(rrs, tableConfig, joinKeyValue);

          if (dataNodeSet.isEmpty()) {
            throw new SQLNonTransientException("parent key can't find any valid datanode ");
          }
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "found partion nodes (using parent partion rule directly) for child table to update  "
                    + Arrays.toString(dataNodeSet.toArray()) + " sql :" + sql);
          }
          if (dataNodeSet.size() > 1) {
            routeToMultiNode(rrs.isCacheAble(), rrs, dataNodeSet, sql, null);
            rrs.setFinishedRoute(true);
            return;
          } else {
            rrs.setCacheAble(true);
            routeToSingleNode(rrs, dataNodeSet.iterator().next(), sql);
            return;
          }

        } else {
          // #####
          // 没找到拆分字段，是分库分表，查询用的不是分库分表字段，该表的所有节点都路由
          if (tablesRouteMap.get(tableName) == null) {
            tablesRouteMap.put(tableName, new HashSet<String>());
            RuleAlgorithm dmdsAlg = tableConfig.getRule().getRuleAlgorithm();
            Integer dbCount = dmdsAlg.getDbCount();
            Integer tbCount = dmdsAlg.getTbCount();
            if (dbCount > 1 || tbCount > 1) {
              ArrayList<String> dataNodes = tableConfig.getDataNodes();
              tablesRouteMap.get(tableName).addAll(
                  getFullDataNodes(tableName, tableConfig.getRule().getRuleAlgorithm(), dataNodes));
            }
          } else {
            tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
          }
        }
      }
    }
  }

  /**
   * @param tableName
   * @param algorithm
   * @param dataNodes
   * @return
   * @author niuzichun
   * <p>
   * 获取全部的节点信息，数据库全扫描 在需要
   */
  public static Set<String> getFullDataNodes(String tableName, RuleAlgorithm algorithm,
      ArrayList<String> dataNodes) {
    Set<String> nodes = new HashSet<String>();
    if (CollectionUtils.isEmpty(dataNodes)) {
      throw new SqlParseException("数据表: " + tableName + " 缺乏，数据节点配置");
    }

    // 单库单表或者没有分表的全局表，单表
    // guoweidong修改增加 algorithm==null判断 是否是单库表
    if (algorithm == null || (algorithm.getDbCount() < 2 && algorithm.getTbCount() < 2)) {
      for (String node : dataNodes) {
        StringBuilder nodeValue = new StringBuilder();
        nodeValue.append(node);
        nodes.add(nodeValue.toString());
      }
    } else {
      int tabeTotalCount = algorithm.getDbCount() * algorithm.getTbCount();
      for (int i = 0; i < tabeTotalCount; i++) {
        int tableIndex = algorithm.calculateTableForTableIndex(i);
        int dbIndex = algorithm.calculateForDBIndex(tableIndex);
        StringBuilder nodeValue = new StringBuilder();
        nodeValue.append(dataNodes.get(dbIndex));
        nodeValue.append(DmdsConstants.seq_db_tb);
        nodeValue.append(TbSeqUtil.getDivTableName(tableName, tableIndex));
        nodes.add(nodeValue.toString());
      }
    }

    return nodes;
  }

  public static boolean isAllGlobalTable(DruidShardingParseInfo ctx, SchemaConfig schema) {
    boolean isAllGlobal = false;
    for (String table : ctx.getTables()) {
      TableConfig tableConfig = schema.getTables().get(table);
      if (tableConfig != null && tableConfig.isGlobalTable()) {
        isAllGlobal = true;
      } else {
        return false;
      }
    }
    return isAllGlobal;
  }

  /**
   * @param schema
   * @param ctx
   * @param tc
   * @return true表示校验通过，false表示检验不通过
   */
  public static boolean checkRuleRequired(SchemaConfig schema, DruidShardingParseInfo ctx,
      RouteCalculateUnit routeUnit, TableConfig tc) {
    if (!tc.isRuleRequired()) {
      return true;
    }
    boolean hasRequiredValue = false;
    String tableName = tc.getName();
    if (routeUnit.getTablesAndConditions().get(tableName) == null
        || routeUnit.getTablesAndConditions().get(tableName).size() == 0) {
      hasRequiredValue = false;
    } else {
      for (Map.Entry<String, Set<ColumnRoutePair>> condition : routeUnit.getTablesAndConditions()
          .get(tableName)
          .entrySet()) {

        String colName = condition.getKey();
        // 条件字段是拆分字段
        if (colName.equals(tc.getPartitionColumn())) {
          hasRequiredValue = true;
          break;
        }
      }
    }
    return hasRequiredValue;
  }

  /**
   * 增加判断支持未配置分片的表走默认的dataNode
   *
   * @param schemaConfig
   * @param tableName
   * @return
   */
  public static boolean isNoSharding(SchemaConfig schemaConfig, String tableName) {
    // Table名字被转化为大写的，存储在schema
    tableName = tableName.toUpperCase();
    if (schemaConfig.isNoSharding()) {
      return true;
    }

    if (schemaConfig.getDataNode() != null && !schemaConfig.getTables().containsKey(tableName)) {
      return true;
    }

    return false;
  }

  /**
   * 判断条件是否永真
   *
   * @param expr
   * @return
   */
  public static boolean isConditionAlwaysTrue(SQLExpr expr) {
    Object o = WallVisitorUtils.getValue(expr);
    if (Boolean.TRUE.equals(o)) {
      return true;
    }
    return false;
  }

  /**
   * 判断条件是否永假的
   *
   * @param expr
   * @return
   */
  public static boolean isConditionAlwaysFalse(SQLExpr expr) {
    Object o = WallVisitorUtils.getValue(expr);
    if (Boolean.FALSE.equals(o)) {
      return true;
    }
    return false;
  }

  /**
   * 根据 ER分片规则获取路由集合
   *
   * @param stmt       执行的语句
   * @param rrs        数据路由集合
   * @param tc         表实体
   * @param joinKeyVal 连接属性
   * @return RouteResultset(数据路由集合) *
   * @throws java.sql.SQLNonTransientException
   */

  public static RouteResultset routeByERParentKey(IServerConnection sc, SchemaConfig schema,
      int sqlType, String stmt,
      RouteResultset rrs, TableConfig tc, String joinKeyVal) throws SQLNonTransientException {

    // only has one parent level and ER parent key is parent
    // table's partition key
    if (tc.isSecondLevel() && tc.getParentTC().getPartitionColumn()
        .equals(tc.getParentKey())) { // using
      // parent
      // rule
      // to
      // find
      // datanode
      Set<ColumnRoutePair> parentColVal = new HashSet<ColumnRoutePair>(1);
      ColumnRoutePair pair = new ColumnRoutePair(joinKeyVal);
      parentColVal.add(pair);
      Set<String> dataNodeSet = ruleCalculate(tc.getParentTC(), parentColVal);
      if (dataNodeSet.isEmpty() || dataNodeSet.size() > 1) {
        throw new SQLNonTransientException(
            "parent key can't find  valid datanode ,expect 1 but found: " + dataNodeSet.size());
      }
      String dn = dataNodeSet.iterator().next();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "found partion node (using parent partion rule directly) for child table to insert  "
                + dn
                + " sql :" + stmt);
      }
      return DmdsRouterUtil.routeToSingleNode(rrs, dn, stmt);
    }
    return null;
  }

}
