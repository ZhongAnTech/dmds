/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio.handler;

import com.zhongan.dmds.core.BackendConnection;

public interface LoadDataResponseHandler {

  /**
   * 收到请求发送文件数据包的响应处理
   */
  void requestDataResponse(byte[] row, BackendConnection conn);
}
