/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.util;

import com.zhongan.dmds.commons.util.sql.MySQLTableReplaceUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 分表工具类
 *
 * @author nzc
 */
public class TbSeqUtil {

  private final static String table_prefix = "0000";
  private final static int table_prefix_length = table_prefix.length();

  /**
   * 获取分表的表名
   *
   * @param tableName
   * @param index
   */
  public static String getDivTableName(String tableName, Integer index) {

    if (index == null) {
      return tableName;
    }

    // 增加 判断表名是否包含·符号的判断。
    String newTableName = tableName;
    if (tableName.startsWith("`") && tableName.endsWith("`")) {
      newTableName = tableName.substring(1, tableName.length() - 1);
    }
    return buildStr(newTableName, "_",
        StringUtils.leftPad(index.toString(), table_prefix_length, "0"));

  }

  public static String buildStr(String... strs) {
    StringBuilder build = new StringBuilder();
    for (String str : strs) {
      build.append(str);
    }
    return build.toString();
  }

  @Deprecated
  /**
   * Hash bug JCPT-2861
   */
  public static String replaceDivTableName(String orgSql, String tableNameNew) {
    if (tableNameNew == null || tableNameNew.trim().length() == 0) {
      return orgSql;
    }

    if (orgSql.indexOf(tableNameNew.toLowerCase()) != -1
        || orgSql.indexOf(tableNameNew.toUpperCase()) != -1) {
      return orgSql;
    }
    String oldTableName = tableNameNew
        .substring(0, tableNameNew.length() - table_prefix_length - 1);

    return replaceDivTableName(orgSql, tableNameNew, oldTableName);
  }

  /**
   * Replace logic table name to sharding table name
   *
   * @param sql        original sql
   * @param tableNames sharding table name [tablename_xxxx]*
   * @return replaced sql
   */
  public static String replaceShardingTableNames(String sql, String[] tableNames) {
    if (tableNames == null || tableNames.length == 0) {
      return sql;
    }
    boolean containsTableName = false;
    String upperCaseSQL = sql.toLowerCase();
    Map<String, String> tableMap = new HashMap<>();
    for (String tableName : tableNames) {
      String logicTableName = tableName.substring(0, tableName.length() - table_prefix_length - 1);
      tableMap.put(logicTableName, tableName.toLowerCase());
      if (!containsTableName && upperCaseSQL.contains(logicTableName.toLowerCase())) {
        containsTableName = true;
      }
    }
    if (!containsTableName) {
      return sql;
    }
    return MySQLTableReplaceUtil.replace(sql, tableMap);
  }

  /**
   * 替换原始语句中的表名为分表的表名
   *
   * @param orgSql
   * @param tableNameNew
   * @param tableNameOld
   * @return
   */

  public static String replaceDivTableName(String orgSql, String tableNameNew,
      String tableNameOld) {
    return replaceSql(orgSql, tableNameOld.toUpperCase(), tableNameNew.toLowerCase());
  }

  /**
   * 替换表名字，在sql语句中会出现对库、表``符号引用，所以在替换前需要处理
   *
   * @param sql
   * @param oldName
   * @param newName
   * @return
   */
  private static String replaceSql(String sql, String oldName, String newName) {
    sql = sql.replaceAll("(?i)[\\s|.]+[`]?" + oldName + "[`]?[(]{1}", " " + newName + " (");
    sql = sql.replaceAll("(?i)[\\s|.]+[`]?" + oldName + "[`]?[.]{1}", " " + newName + ".");
    sql = sql.replaceAll("(?i)[\\s|.]+[`]?" + oldName + "[`]?[\\s|;]+", " " + newName + " ");
    sql = sql.replaceAll("(?i)[\\s|.]+[`]?" + oldName + "[`]?$", " " + newName + " ");
    return sql;
  }

  public static void main(String[] args) {
    // long start=System.currentTimeMillis();
    // for(int i=0;i<10000;i++){
    // //System.out.println(TbSeqUtil.replaceDivTableName("select * from
    // tb","tb_0001"));
    // // String
    // oldTableName="tb_0001".substring(0,"tb_0001".length()-table_prefix_length-1);
    // TbSeqUtil.buildStr(System.currentTimeMillis()+"",System.currentTimeMillis()+"","test3");
    // //String t1=System.currentTimeMillis()+""+System.currentTimeMillis()+"test3";
    // }
    // System.out.println("time:"+(System.currentTimeMillis()-start));

    // String sql="CREATE TABLE `order` ( `id` BIGINT NOT NULL AUTO_INCREMENT,
    // `name` VARCHAR (64) NOT NULL, `gender` TINYINT NOT NULL DEFAULT 1, `birthday`
    // DATETIME NOT NULL, `group` VARCHAR (64) NOT NULL DEFAULT 'za', `create_time`
    // DATETIME NOT NULL DEFAULT now(), PRIMARY KEY (`id`) ) ";
    String sql = "ALTER TABLE demo\n\rADD COLUMN mm7 varchar(50) NULL";
    String oldName = "demo";
    String newName = "demo_0002";

    sql = replaceSql(sql, oldName, newName);
    System.out.println(sql);
  }

}
