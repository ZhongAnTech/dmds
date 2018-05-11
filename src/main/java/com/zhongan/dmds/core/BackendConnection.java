/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.zhongan.dmds.route.RouteResultsetNode;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public interface BackendConnection extends ClosableConnection {

  public boolean isModifiedSQLExecuted();

  public boolean isFromSlaveDB();

  public String getSchema();

  public void setSchema(String newSchema);

  public long getLastTime();

  public boolean isClosedOrQuit();

  public void setAttachment(Object attachment);

  public void quit();

  public void setLastTime(long currentTimeMillis);

  public void release();

  public boolean setResponseHandler(ResponseHandler commandHandler);

  public void commit();

  public void query(String sql) throws UnsupportedEncodingException;

  public Object getAttachment();

  public void execute(RouteResultsetNode node, IServerConnection source, boolean autocommit)
      throws IOException;

  public void recordSql(String host, String schema, String statement);

  public boolean syncAndExcute();

  public void rollback();

  public boolean isBorrowed();

  /**
   * 是否需要从pool中取新连接
   *
   * @param borrowed
   */
  public void setBorrowed(boolean borrowed);

  public int getTxIsolation();

  public boolean isAutocommit();

  public long getId();

  public void discardClose(String reason);

}
