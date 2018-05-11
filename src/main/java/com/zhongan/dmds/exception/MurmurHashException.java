/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.exception;

public class MurmurHashException extends RuntimeException {

  /**
   *
   */
  private static final long serialVersionUID = -8040964553491203562L;

  public MurmurHashException() {
    super();

  }

  public MurmurHashException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);

  }

  public MurmurHashException(String message, Throwable cause) {
    super(message, cause);

  }

  public MurmurHashException(String message) {
    super(message);

  }

  public MurmurHashException(Throwable cause) {
    super(cause);

  }

}
