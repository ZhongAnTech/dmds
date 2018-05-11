/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net;

import com.zhongan.dmds.core.SocketWR;

public class WriteEventCheckRunner implements Runnable {

  private final SocketWR socketWR;
  private volatile boolean finshed = true;

  public WriteEventCheckRunner(SocketWR socketWR) {
    this.socketWR = socketWR;
  }

  public boolean isFinished() {
    return finshed;
  }

  @Override
  public void run() {
    finshed = false;
    socketWR.doNextWriteCheck();
    finshed = true;
  }
}