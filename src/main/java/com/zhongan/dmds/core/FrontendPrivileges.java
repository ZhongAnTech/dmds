/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import java.util.Set;

/**
 * 权限提供者
 */
public interface FrontendPrivileges {

  /**
   * 检查schema是否存在
   */
  boolean schemaExists(String schema);

  /**
   * 检查用户是否存在，并且可以使用host实行隔离策略。
   */
  boolean userExists(String user, String host);

  /**
   * 提供用户的服务器端密码
   */
  String getPassword(String user);

  /**
   * 提供有效的用户schema集合
   */
  Set<String> getUserSchemas(String user);

  /**
   * 检查用户是否为只读权限
   *
   * @param user
   * @return
   */
  Boolean isReadOnly(String user);

  /**
   * 检查用户当系统有效使用的负载 百分比
   *
   * @param user
   * @return
   */
  int getBenchmark(String user);

  /**
   * 负载拒连后 的短信预警
   *
   * @param user
   * @return
   */
  String getBenchmarkSmsTel(String user);

}