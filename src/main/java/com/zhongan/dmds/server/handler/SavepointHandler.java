/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.handler;

import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.core.IServerConnection;

public final class SavepointHandler {

  public static void handle(String stmt, IServerConnection c) {
    c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported statement");
  }

}