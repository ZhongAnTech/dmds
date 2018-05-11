/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config;

/**
 * DMDS报警关键词定义
 */
public interface Alarms {

  /**
   * 默认报警关键词
   **/
  public static final String DEFAULT = "#!DMDS#";

  /**
   * 集群无有效的节点可提供服务
   **/
  public static final String CLUSTER_EMPTY = "#!CLUSTER_EMPTY#";

  /**
   * 数据节点的数据源发生切换
   **/
  public static final String DATANODE_SWITCH = "#!DN_SWITCH#";

  /**
   * 隔离区非法用户访问
   **/
  public static final String QUARANTINE_ATTACK = "#!QT_ATTACK#";

}
