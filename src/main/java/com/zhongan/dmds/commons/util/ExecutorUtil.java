/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.util;

import java.util.concurrent.LinkedTransferQueue;

public class ExecutorUtil {

  public static final NameableExecutor create(String name, int size) {
    return create(name, size, true);
  }

  private static final NameableExecutor create(String name, int size, boolean isDaemon) {
    NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
    return new NameableExecutor(name, size, new LinkedTransferQueue<Runnable>(), factory);
  }

}