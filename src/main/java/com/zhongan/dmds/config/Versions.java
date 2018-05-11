/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config;

/**
 * change version to dmds
 */
public abstract class Versions {

  /**
   * 协议版本
   **/
  public static final byte PROTOCOL_VERSION = 10;

  /**
   * 服务器版本
   **/
  public static byte[] SERVER_VERSION = "5.5.8-dmds-1.0.0-RELEASE-20170619102622".getBytes();

  public static void setServerVersion(String version) {
    byte[] mysqlVersionPart = version.getBytes();
    int startIndex;
    for (startIndex = 0; startIndex < SERVER_VERSION.length; startIndex++) {
      if (SERVER_VERSION[startIndex] == '-') {
        break;
      }
    }

    // 重新拼接dmds version字节数组
    byte[] newDmdsVersion = new byte[mysqlVersionPart.length + SERVER_VERSION.length - startIndex];
    System.arraycopy(mysqlVersionPart, 0, newDmdsVersion, 0, mysqlVersionPart.length);
    System.arraycopy(SERVER_VERSION, startIndex, newDmdsVersion, mysqlVersionPart.length,
        SERVER_VERSION.length - startIndex);
    SERVER_VERSION = newDmdsVersion;
  }
}
