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

public class SimpleLogHandler implements ResponseHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleLogHandler.class);

  @Override
  public void connectionError(Throwable e, BackendConnection conn) {
    LOGGER.warn(conn + " connectionError " + e);

  }

  @Override
  public void connectionAcquired(BackendConnection conn) {
    LOGGER.info("connectionAcquired " + conn);

  }

  @Override
  public void errorResponse(byte[] err, BackendConnection conn) {
    LOGGER.warn("caught error resp: " + conn + " " + new String(err));
  }

  @Override
  public void okResponse(byte[] ok, BackendConnection conn) {
    LOGGER.info("okResponse: " + conn);

  }

  @Override
  public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof,
      BackendConnection conn) {
    LOGGER.info("fieldEofResponse: " + conn);

  }

  @Override
  public void rowResponse(byte[] row, BackendConnection conn) {
    LOGGER.info("rowResponse: " + conn);

  }

  @Override
  public void rowEofResponse(byte[] eof, BackendConnection conn) {
    LOGGER.info("rowEofResponse: " + conn);

  }

  @Override
  public void writeQueueAvailable() {

  }

  @Override
  public void connectionClose(BackendConnection conn, String reason) {

  }

}