/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.util;

/**
 * 弱精度的计时器，考虑性能不使用同步策略。
 */
public class TimeUtil {

  private static volatile long CURRENT_TIME = System.currentTimeMillis();

  public static final long currentTimeMillis() {
    return CURRENT_TIME;
  }

  public static final long currentTimeNanos() {
    return System.nanoTime();
  }

  public static final void update() {
    CURRENT_TIME = System.currentTimeMillis();
  }

}