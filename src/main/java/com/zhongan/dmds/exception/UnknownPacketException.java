/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.exception;

/**
 * 未知数据包异常
 */
public class UnknownPacketException extends RuntimeException {

  private static final long serialVersionUID = 3152986441780514147L;

  public UnknownPacketException() {
    super();
  }

  public UnknownPacketException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnknownPacketException(String message) {
    super(message);
  }

  public UnknownPacketException(Throwable cause) {
    super(cause);
  }

}