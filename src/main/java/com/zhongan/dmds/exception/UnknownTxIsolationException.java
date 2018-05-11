/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.exception;

/**
 * 未知事物隔离级别异常
 */
public class UnknownTxIsolationException extends RuntimeException {

  private static final long serialVersionUID = -3911059999308980358L;

  public UnknownTxIsolationException() {
    super();
  }

  public UnknownTxIsolationException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnknownTxIsolationException(String message) {
    super(message);
  }

  public UnknownTxIsolationException(Throwable cause) {
    super(cause);
  }

}