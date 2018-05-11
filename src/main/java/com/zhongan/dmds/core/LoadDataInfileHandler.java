/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

/**
 * load data infile
 */
public interface LoadDataInfileHandler {

  void start(String sql);

  void handle(byte[] data);

  void end(byte packID);

  void clear();

  byte getLastPackId();

  boolean isStartLoadData();

}