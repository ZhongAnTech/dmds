/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import java.util.concurrent.Executor;

public abstract class BackendAsyncHandler implements NIOHandler {

  protected void offerData(byte[] data, Executor executor) {
    handleData(data);
  }

  protected abstract void offerDataError();

  protected abstract void handleData(byte[] data);

}