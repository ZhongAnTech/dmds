/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route;

import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.TableConfig;
import com.zhongan.dmds.config.util.StringUtil;
import com.zhongan.dmds.core.IServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;

/**
 * 从ServerRouterUtil中抽取的一些公用方法，路由解析工具类
 * 2016.12 dmds增加分表功能
 */
public class RouterUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(RouterUtil.class);

  public static boolean processInsert(SchemaConfig schema, int sqlType, String origSQL,
      IServerConnection sc)
      throws SQLNonTransientException {
    String tableName = StringUtil.getTableName(origSQL).toUpperCase();
    TableConfig tableConfig = schema.getTables().get(tableName);
    boolean processedInsert = false;
    if (null != tableConfig && tableConfig.isAutoIncrement()) {
      String primaryKey = tableConfig.getPrimaryKey();
      processedInsert = processInsert(sc, schema, sqlType, origSQL, tableName, primaryKey);
    }
    return processedInsert;
  }

  private static boolean isPKInFields(String origSQL, String primaryKey, int firstLeftBracketIndex,
      int firstRightBracketIndex) {

    if (primaryKey == null) {
      throw new RuntimeException(
          "please make sure the primaryKey's config is not null in schemal.xml");
    }

    boolean isPrimaryKeyInFields = false;
    String upperSQL = origSQL.substring(firstLeftBracketIndex, firstRightBracketIndex + 1)
        .toUpperCase();
    for (int pkOffset = 0, primaryKeyLength = primaryKey.length(), pkStart = 0; ; ) {
      pkStart = upperSQL.indexOf(primaryKey, pkOffset);
      if (pkStart >= 0 && pkStart < firstRightBracketIndex) {
        char pkSide = upperSQL.charAt(pkStart - 1);
        if (pkSide <= ' ' || pkSide == '`' || pkSide == ',' || pkSide == '(') {
          pkSide = upperSQL.charAt(pkStart + primaryKey.length());
          isPrimaryKeyInFields = pkSide <= ' ' || pkSide == '`' || pkSide == ',' || pkSide == ')';
        }
        if (isPrimaryKeyInFields) {
          break;
        }
        pkOffset = pkStart + primaryKeyLength;
      } else {
        break;
      }
    }
    return isPrimaryKeyInFields;
  }

  public static boolean processInsert(IServerConnection sc, SchemaConfig schema, int sqlType,
      String origSQL,
      String tableName, String primaryKey) throws SQLNonTransientException {
    int firstLeftBracketIndex = origSQL.indexOf("(");
    int firstRightBracketIndex = origSQL.indexOf(")");
    String upperSql = origSQL.toUpperCase();
    int valuesIndex = upperSql.indexOf("VALUES");
    int selectIndex = upperSql.indexOf("SELECT");
    int fromIndex = upperSql.indexOf("FROM");
    if (firstLeftBracketIndex < 0) {// insert into table1 select * from
      // table2
      String msg = "invalid sql:" + origSQL;
      LOGGER.warn(msg);
      throw new SQLNonTransientException(msg);
    }

    if (selectIndex > 0 && fromIndex > 0 && selectIndex > firstRightBracketIndex
        && valuesIndex < 0) {
      String msg = "multi insert not provided";
      LOGGER.warn(msg);
      throw new SQLNonTransientException(msg);
    }

    if (valuesIndex + "VALUES".length() <= firstLeftBracketIndex) {
      throw new SQLSyntaxErrorException("insert must provide ColumnList");
    }

    boolean processedInsert = !isPKInFields(origSQL, primaryKey, firstLeftBracketIndex,
        firstRightBracketIndex);
    if (processedInsert) {
      processInsert(sc, schema, sqlType, origSQL, tableName, primaryKey, firstLeftBracketIndex + 1,
          origSQL.indexOf('(', firstRightBracketIndex) + 1);
    }
    return processedInsert;
  }

  private static void processInsert(IServerConnection sc, SchemaConfig schema, int sqlType,
      String origSQL,
      String tableName, String primaryKey, int afterFirstLeftBracketIndex,
      int afterLastLeftBracketIndex) {

    int primaryKeyLength = primaryKey.length();
    int insertSegOffset = afterFirstLeftBracketIndex;
    String seqPrefix = "next value for MYCATSEQ_";
    int seqPrefixLength = seqPrefix.length();
    int tableNameLength = tableName.length();

    char[] newSQLBuf = new char[origSQL.length() + primaryKeyLength + seqPrefixLength
        + tableNameLength + 2];
    origSQL.getChars(0, afterFirstLeftBracketIndex, newSQLBuf, 0);
    primaryKey.getChars(0, primaryKeyLength, newSQLBuf, insertSegOffset);
    insertSegOffset += primaryKeyLength;
    newSQLBuf[insertSegOffset] = ',';
    insertSegOffset++;
    origSQL.getChars(afterFirstLeftBracketIndex, afterLastLeftBracketIndex, newSQLBuf,
        insertSegOffset);
    insertSegOffset += afterLastLeftBracketIndex - afterFirstLeftBracketIndex;
    seqPrefix.getChars(0, seqPrefixLength, newSQLBuf, insertSegOffset);
    insertSegOffset += seqPrefixLength;
    tableName.getChars(0, tableNameLength, newSQLBuf, insertSegOffset);
    insertSegOffset += tableNameLength;
    newSQLBuf[insertSegOffset] = ',';
    insertSegOffset++;
    origSQL.getChars(afterLastLeftBracketIndex, origSQL.length(), newSQLBuf, insertSegOffset);
  }
}
