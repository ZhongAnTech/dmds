/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route;

import java.sql.Types;

public class ProcedureParameter {

  public static final String IN = "in";
  public static final String OUT = "out";
  public static final String INOUT = "inout";

  private int index;
  private String name;

  // in out inout
  private String parameterType;

  // java.sql.Types
  private int jdbcType = Types.VARCHAR;

  private Object value;

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getParameterType() {
    return parameterType;
  }

  public void setParameterType(String parameterType) {
    this.parameterType = parameterType;
  }

  public int getJdbcType() {
    return jdbcType;
  }

  public void setJdbcType(int jdbcType) {
    this.jdbcType = jdbcType;
  }
}
