/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Condition;
import com.alibaba.fastjson.JSON;
import com.zhongan.dmds.cache.LayerCachePool;
import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.mpp.RangeValue;
import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.sqlParser.druid.DmdsSchemaStatVisitor;
import com.zhongan.dmds.sqlParser.druid.DruidParser;
import com.zhongan.dmds.sqlParser.druid.DruidShardingParseInfo;
import com.zhongan.dmds.sqlParser.druid.RouteCalculateUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对SQLStatement解析 主要通过visitor解析和statement解析：有些类型的SQLStatement通过visitor解析足够了，
 * 有些只能通过statement解析才能得到所有信息 有些需要通过两种方式解析才能得到完整信息
 *
 * 2017.04 移除对ER分片childTable的支持
 */
public class DefaultDruidParser implements DruidParser {

  protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultDruidParser.class);
  /**
   * 解析得到的结果
   */
  protected DruidShardingParseInfo ctx;

  private Map<String, String> tableAliasMap = new HashMap<String, String>();

  public Map<String, String> getTableAliasMap() {
    return tableAliasMap;
  }

  /**
   * 使用DmdsSchemaStatVisitor解析,得到tables、tableAliasMap、conditions等
   *
   * @param schema
   * @param stmt
   */
  public void parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql,
      LayerCachePool cachePool, DmdsSchemaStatVisitor schemaStatVisitor)
      throws SQLNonTransientException {
    ctx = new DruidShardingParseInfo();
    // 设置为原始sql，如果有需要改写sql的，可以通过修改SQLStatement中的属性，然后调用SQLStatement.toString()得到改写的sql
    ctx.setSql(originSql);
    // 通过visitor解析
    visitorParse(rrs, stmt, schemaStatVisitor);
    // 通过Statement解析
    statementParse(schema, rrs, stmt);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(String.valueOf(rrs.isFinishedRoute()));
    }
    // 改写sql：如insert语句主键自增长的可以
    changeSql(schema, rrs, stmt, cachePool);
  }

  /**
   * 子类可覆盖（如果visitorParse解析得不到表名、字段等信息的，就通过覆盖该方法来解析） 子类覆盖该方法一般是将SQLStatement转型后再解析（如转型为MySqlInsertStatement）
   */
  @Override
  public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt)
      throws SQLNonTransientException {

  }

  /**
   * 改写sql：如insert是
   */
  @Override
  public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,
      LayerCachePool cachePool)
      throws SQLNonTransientException {

  }

  /**
   * 子类可覆盖（如果该方法解析得不到表名、字段等信息的，就覆盖该方法，覆盖成空方法，然后通过statementParse去解析） 通过visitor解析：有些类型的Statement通过visitor解析得不到表名、
   *
   * @param stmt
   */
  @Override
  public void visitorParse(RouteResultset rrs, SQLStatement stmt, DmdsSchemaStatVisitor visitor)
      throws SQLNonTransientException {

    stmt.accept(visitor);

    List<List<Condition>> mergedConditionList = new ArrayList<List<Condition>>();
    if (visitor.hasOrCondition()) {// 包含or语句
      // 根据or拆分
      mergedConditionList = visitor.splitConditions();
    } else {// 不包含OR语句
      mergedConditionList.add(visitor.getConditions());
    }

    if (visitor.getAliasMap() != null) {
      for (Map.Entry<String, String> entry : visitor.getAliasMap().entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (key != null && key.indexOf("`") >= 0) {
          key = key.replaceAll("`", "");
        }
        if (value != null && value.indexOf("`") >= 0) {
          value = value.replaceAll("`", "");
        }
        // 表名前面带database的，去掉
        if (key != null) {
          int pos = key.indexOf(".");
          if (pos > 0) {
            key = key.substring(pos + 1);
          }
        }

        if (key.equals(value)) {
          ctx.addTable(key.toUpperCase());
        }
        // else {
        // tableAliasMap.put(key, value);
        // }
        tableAliasMap.put(key.toUpperCase(), value);
      }
      visitor.getAliasMap().putAll(tableAliasMap);
      ctx.setTableAliasMap(tableAliasMap);
    }
    LOGGER.debug("mergedConditionList:" + JSON.toJSONString(mergedConditionList));
    ctx.setRouteCalculateUnits(this.buildRouteCalculateUnits(visitor, mergedConditionList));
  }

  private List<RouteCalculateUnit> buildRouteCalculateUnits(SchemaStatVisitor visitor,
      List<List<Condition>> conditionList) {
    List<RouteCalculateUnit> retList = new ArrayList<RouteCalculateUnit>();
    // 遍历condition ，找分片字段
    for (int i = 0; i < conditionList.size(); i++) {
      RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
      for (Condition condition : conditionList.get(i)) {
        List<Object> values = condition.getValues();
        if (values.size() == 0) {
          break;
        }
        if (checkConditionValues(values)) {
          String columnName = StringUtil
              .removeBackquote(condition.getColumn().getName().toUpperCase());
          String tableName = StringUtil
              .removeBackquote(condition.getColumn().getTable().toUpperCase());

          if (visitor.getAliasMap() != null && visitor.getAliasMap().get(tableName) != null
              && !visitor.getAliasMap().get(tableName).equals(tableName)) {
            tableName = visitor.getAliasMap().get(tableName);
          }

          // if(visitor.getAliasMap() != null &&
          // visitor.getAliasMap().get(condition.getColumn().getTable().toUpperCase()) ==
          // null) {//子查询的别名条件忽略掉,不参数路由计算，否则后面找不到表
          if (visitor.getAliasMap() != null && visitor.getAliasMap()
              .get(StringUtil.removeBackquote(condition.getColumn().getTable().toUpperCase()))
              == null) {// 子查询的别名条件忽略掉,不参数路由计算，否则后面找不到表
            continue;
          }

          String operator = condition.getOperator();

          // 只处理between ,in和=3中操作符
          if (operator.equals("between")) {
            RangeValue rv = new RangeValue(values.get(0), values.get(1), RangeValue.EE);
            routeCalculateUnit.addShardingExpr(tableName.toUpperCase(), columnName, rv);
          } else if (operator.equals("=") || operator.toLowerCase()
              .equals("in")) { // 只处理=号和in操作符,其他忽略
            routeCalculateUnit
                .addShardingExpr(tableName.toUpperCase(), columnName, values.toArray());
          }
        }
      }
      retList.add(routeCalculateUnit);
    }
    return retList;
  }

  private boolean checkConditionValues(List<Object> values) {
    for (Object value : values) {
      if (value != null && !value.toString().equals("")) {
        return true;
      }
    }
    return false;
  }

  public DruidShardingParseInfo getCtx() {
    return ctx;
  }
}
