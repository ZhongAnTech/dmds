/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.contants.mysql;

/**
 * hit grammer mycat -> dmds
 * add sharding table spliter
 */
public class DmdsConstants {

  //PhysicalDBPool
  public static final int BALANCE_NONE = 0;
  public static final int BALANCE_ALL_BACK = 1;
  public static final int BALANCE_ALL = 2;
  public static final int BALANCE_ALL_READ = 3;

  public static final int WRITE_ONLYONE_NODE = 0;
  public static final int WRITE_RANDOM_NODE = 1;
  public static final int WRITE_ALL_NODE = 2;
  public static final int WEIGHT = 0;
  public static final long LONG_TIME = 300000;

  //datahostconfig
  public static final int NOT_SWITCH_DS = -1;
  public static final int DEFAULT_SWITCH_DS = 1;
  public static final int SYN_STATUS_SWITCH_DS = 2;
  public static final int CLUSTER_STATUS_SWITCH_DS = 3;

  //分库分表
  public static final String seq_db_tb = "#"; //库表连接串分隔符

  //hint
  public static final String HINT = "/*!dmds:";
  public static final String HINT_SPLIT = "=";


}
