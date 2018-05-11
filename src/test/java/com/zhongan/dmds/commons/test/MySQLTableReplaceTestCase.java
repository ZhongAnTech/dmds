/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.test;

import com.zhongan.dmds.commons.util.sql.MySQLTableReplaceUtil;

import java.util.HashMap;
import java.util.Map;

public class MySQLTableReplaceTestCase {

  private String name;

  private String sourceSql;

  private Map<String, String> tableMap;

  private String validateSql;

  private MySQLTableReplaceTestCase(String name, String sourceSql, String tableMapStr,
      String validateSql) {
    this.name = name;
    this.sourceSql = sourceSql;
    this.validateSql = validateSql;
    this.tableMap = new HashMap<>();
    if (tableMapStr != null) {
      String[] valuePairs = tableMapStr.split(",");
      for (String valuePair : valuePairs) {
        String[] keyAndValue = valuePair.split(":");
        if (keyAndValue.length != 2) {
          throw new IllegalArgumentException("Illegal format : " + valuePair);
        }
        tableMap.put(keyAndValue[0].trim(), keyAndValue[1]);
      }
    }
  }

  public static MySQLTableReplaceTestCase of(String name, String sourceSql, String validateSql,
      String tableMapStr) {
    return new MySQLTableReplaceTestCase(name, sourceSql, validateSql, tableMapStr);
  }

  public String getName() {
    return this.name;
  }

  public boolean test() {
    System.out.println("Test : " + this.name);
    String replacedSQL = MySQLTableReplaceUtil.replace(this.sourceSql, tableMap);
    boolean validate = validateSql.equals(replacedSQL);
    if (!validate) {
      System.out.println("ValidateSQL :" + validateSql);
      System.out.println("ReplacedSQL :" + replacedSQL);
    }
    return validate;
  }

  public String getSourceSql() {
    return sourceSql;
  }

  public Map<String, String> getTableMap() {
    return tableMap;
  }

  public String getValidateSql() {
    return validateSql;
  }
}
