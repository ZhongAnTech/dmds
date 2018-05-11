/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio.handler;

import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RollbackReleaseHandler implements ResponseHandler {

  private static final Logger logger = LoggerFactory.getLogger(RollbackReleaseHandler.class);

  public RollbackReleaseHandler() {
  }

  @Override
  public void connectionAcquired(BackendConnection conn) {
    logger.error("unexpected invocation: connectionAcquired from rollback-release");
  }

  @Override
  public void connectionError(Throwable e, BackendConnection conn) {

  }

  @Override
  public void errorResponse(byte[] err, BackendConnection conn) {
    conn.quit();
  }

  @Override
  public void okResponse(byte[] ok, BackendConnection conn) {
    logger.debug(
        "autocomit is false,but no commit or rollback ,so dmds rollbacked backend conn " + conn);
    conn.release();
  }

  @Override
  public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof,
      BackendConnection conn) {
  }

  @Override
  public void rowResponse(byte[] row, BackendConnection conn) {
  }

  @Override
  public void rowEofResponse(byte[] eof, BackendConnection conn) {

  }

  @Override
  public void writeQueueAvailable() {

  }

  @Override
  public void connectionClose(BackendConnection conn, String reason) {

  }

}