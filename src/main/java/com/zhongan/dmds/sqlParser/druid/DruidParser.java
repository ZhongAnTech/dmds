/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.zhongan.dmds.cache.LayerCachePool;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.route.RouteResultset;

import java.sql.SQLNonTransientException;

/**
 * 对SQLStatement解析 主要通过visitor解析和statement解析：有些类型的SQLStatement通过visitor解析足够了，
 * 有些只能通过statement解析才能得到所有信息 有些需要通过两种方式解析才能得到完整信息
 */
public interface DruidParser {

  /**
   * 使用DmdsSchemaStatVisitor解析,得到tables、tableAliasMap、conditions等
   *
   * @param schema
   * @param stmt
   */
  public void parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql,
      LayerCachePool cachePool, DmdsSchemaStatVisitor schemaStatVisitor)
      throws SQLNonTransientException;

  /**
   * statement方式解析 子类可覆盖（如果visitorParse解析得不到表名、字段等信息的，就通过覆盖该方法来解析） 子类覆盖该方法一般是将SQLStatement转型后再解析（如转型为MySqlInsertStatement）
   */
  public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt)
      throws SQLNonTransientException;

  /**
   * 子类可覆盖（如果该方法解析得不到表名、字段等信息的，就覆盖该方法，覆盖成空方法，然后通过statementPparse去解析） 通过visitor解析：有些类型的Statement通过visitor解析得不到表名、
   *
   * @param stmt
   */
  public void visitorParse(RouteResultset rrs, SQLStatement stmt, DmdsSchemaStatVisitor visitor)
      throws SQLNonTransientException;

  /**
   * 改写sql：加limit，加group by、加order by如有些没有加limit的可以通过该方法增加
   *
   * @param schema
   * @param rrs
   * @param stmt
   * @throws SQLNonTransientException
   */
  public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,
      LayerCachePool cachePool) throws SQLNonTransientException;

  /**
   * 获取解析到的信息
   *
   * @return
   */
  public DruidShardingParseInfo getCtx();

}
