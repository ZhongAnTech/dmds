/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config;

/**
 * 事务隔离级别定义
 */
public interface Isolations {

  public static final int READ_UNCOMMITTED = 1;
  public static final int READ_COMMITTED = 2;
  public static final int REPEATED_READ = 3;
  public static final int SERIALIZABLE = 4;

}