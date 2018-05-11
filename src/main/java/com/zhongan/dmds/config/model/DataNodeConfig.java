/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model;

/**
 * 用于描述一个数据节点的配置
 */
public final class DataNodeConfig {

  private final String name;
  private final String database;
  private final String dataHost;

  public DataNodeConfig(String name, String database, String dataHost) {
    super();
    this.name = name;
    this.database = database;
    this.dataHost = dataHost;
  }

  public String getName() {
    return name;
  }

  public String getDatabase() {
    return database;
  }

  public String getDataHost() {
    return dataHost;
  }

}