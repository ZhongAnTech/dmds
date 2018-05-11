/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.sqlengine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public interface SQLJobHandler {

  public static final Logger LOGGER = LoggerFactory.getLogger(SQLJobHandler.class);

  public void onHeader(String dataNode, byte[] header, List<byte[]> fields);

  public boolean onRowData(String dataNode, byte[] rowData);

  public void finished(String dataNode, boolean failed);
}
