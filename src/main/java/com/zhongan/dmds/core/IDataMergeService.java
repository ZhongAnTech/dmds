/*
 * Copyright (C) 2016-2020 zhongan.com
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.zhongan.dmds.route.ColMeta;
import com.zhongan.dmds.route.RouteResultset;

import java.util.Map;

public interface IDataMergeService extends Runnable {

  RouteResultset getRrs();

  void outputMergeResult(byte[] eof);

  void onRowMetaData(Map<String, ColMeta> columToIndx, int fieldCount);

  /**
   * process new record (mysql binary data),if data can output to client ,return true
   *
   * @param dataNode DN's name (data from this dataNode)
   * @param rowData  raw data
   * @param conn
   */
  boolean onNewRecord(String dataNode, byte[] rowData);

  /**
   * release resources
   */
  void clear();

}
