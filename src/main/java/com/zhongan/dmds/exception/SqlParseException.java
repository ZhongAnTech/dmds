/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.exception;

/**
 * Base on MyCat 1.6
 */
public class SqlParseException extends RuntimeException {

  private static final long serialVersionUID = -180146385688342818L;

  public SqlParseException() {
    super();
  }

  public SqlParseException(String message, Throwable cause) {
    super(message, cause);
  }

  public SqlParseException(String message) {
    super(message);
  }

  public SqlParseException(Throwable cause) {
    super(cause);
  }

}