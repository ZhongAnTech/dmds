/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NameableExecutor extends ThreadPoolExecutor {

  protected String name;

  public NameableExecutor(String name, int size, BlockingQueue<Runnable> queue,
      ThreadFactory factory) {
    super(size, size, Long.MAX_VALUE, TimeUnit.NANOSECONDS, queue, factory);
    this.name = name;
  }

  public String getName() {
    return name;
  }

}