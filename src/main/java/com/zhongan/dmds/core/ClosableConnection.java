/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

public interface ClosableConnection {

  String getCharset();

  /**
   * 关闭连接
   */
  void close(String reason);

  boolean isClosed();

  public void idleCheck();

  long getStartupTime();

  String getHost();

  int getPort();

  int getLocalPort();

  long getNetInBytes();

  long getNetOutBytes();
}