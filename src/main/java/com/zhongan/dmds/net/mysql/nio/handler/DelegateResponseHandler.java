/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio.handler;

import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.ResponseHandler;

import java.util.List;

public class DelegateResponseHandler implements ResponseHandler {

  private final ResponseHandler target;

  public DelegateResponseHandler(ResponseHandler target) {
    if (target == null) {
      throw new IllegalArgumentException("delegate is null!");
    }
    this.target = target;
  }

  @Override
  public void connectionAcquired(BackendConnection conn) {
    // 将后端连接的ResponseHandler设置为target
    target.connectionAcquired(conn);
  }

  @Override
  public void connectionError(Throwable e, BackendConnection conn) {
    target.connectionError(e, conn);
  }

  @Override
  public void okResponse(byte[] ok, BackendConnection conn) {
    target.okResponse(ok, conn);
  }

  @Override
  public void errorResponse(byte[] err, BackendConnection conn) {
    target.errorResponse(err, conn);
  }

  @Override
  public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof,
      BackendConnection conn) {
    target.fieldEofResponse(header, fields, eof, conn);
  }

  @Override
  public void rowResponse(byte[] row, BackendConnection conn) {
    target.rowResponse(row, conn);
  }

  @Override
  public void rowEofResponse(byte[] eof, BackendConnection conn) {
    target.rowEofResponse(eof, conn);
  }

  @Override
  public void writeQueueAvailable() {
    target.writeQueueAvailable();

  }

  @Override
  public void connectionClose(BackendConnection conn, String reason) {
    target.connectionClose(conn, reason);
  }

}