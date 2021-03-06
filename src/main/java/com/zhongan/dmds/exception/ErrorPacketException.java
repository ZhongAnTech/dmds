/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.exception;


public class ErrorPacketException extends RuntimeException {

  private static final long serialVersionUID = -2692093550257870555L;

  public ErrorPacketException() {
    super();
  }

  public ErrorPacketException(String message, Throwable cause) {
    super(message, cause);
  }

  public ErrorPacketException(String message) {
    super(message);
  }

  public ErrorPacketException(Throwable cause) {
    super(cause);
  }

}