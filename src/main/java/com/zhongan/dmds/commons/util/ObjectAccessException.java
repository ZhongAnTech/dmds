/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.util;

public class ObjectAccessException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public ObjectAccessException(String message) {
    super(message);
  }

  public ObjectAccessException(String message, Throwable cause) {
    super(message, cause);
  }
}