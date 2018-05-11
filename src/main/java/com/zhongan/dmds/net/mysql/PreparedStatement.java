/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql;

public class PreparedStatement {

  private long id;
  private String statement;
  private int columnsNumber;
  private int parametersNumber;
  private int[] parametersType;

  public PreparedStatement(long id, String statement, int columnsNumber, int parametersNumber) {
    this.id = id;
    this.statement = statement;
    this.columnsNumber = columnsNumber;
    this.parametersNumber = parametersNumber;
    this.parametersType = new int[parametersNumber];
  }

  public long getId() {
    return id;
  }

  public String getStatement() {
    return statement;
  }

  public int getColumnsNumber() {
    return columnsNumber;
  }

  public int getParametersNumber() {
    return parametersNumber;
  }

  public int[] getParametersType() {
    return parametersType;
  }

}