/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.sqlcmd;

import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.Session;

/**
 * sql command like set xxxx ,only return OK /Err Pacakage,can't return restult set
 */
public interface SQLCtrlCommand {

  boolean isAutoClearSessionCons();

  boolean releaseConOnErr();

  boolean relaseConOnOK();

  void sendCommand(Session session, BackendConnection con);

  /**
   * 收到错误数据包的响应处理
   */
  void errorResponse(Session session, byte[] err, int total, int failed);

  /**
   * 收到OK数据包的响应处理
   */
  void okResponse(Session session, byte[] ok);

}
