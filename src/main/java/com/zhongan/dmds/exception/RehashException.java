/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.exception;

public class RehashException extends RuntimeException {

  /**
   *
   */
  private static final long serialVersionUID = 7562724429239862825L;

  public RehashException() {
    super();

  }

  public RehashException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);

  }

  public RehashException(String message, Throwable cause) {
    super(message, cause);

  }

  public RehashException(String message) {
    super(message);

  }

  public RehashException(Throwable cause) {
    super(cause);

  }

}
