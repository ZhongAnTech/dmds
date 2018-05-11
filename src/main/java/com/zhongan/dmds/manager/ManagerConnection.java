/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager;

import com.zhongan.dmds.commons.util.TimeUtil;
import com.zhongan.dmds.net.frontend.FrontendConnection;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

public class ManagerConnection extends FrontendConnection {

  private static final long AUTH_TIMEOUT = 15 * 1000L;

  public ManagerConnection(NetworkChannel channel) throws IOException {
    super(channel);
  }

  @Override
  public boolean isIdleTimeout() {
    if (isAuthenticated) {
      return super.isIdleTimeout();
    } else {
      return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + AUTH_TIMEOUT;
    }
  }

  @Override
  public void handle(final byte[] data) {
    handler.handle(data);
  }

}