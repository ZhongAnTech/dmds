/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.zhongan.dmds.config.model.SchemaConfig;

/**
 * Extract from ServerConnection
 */
public interface IServerConnection extends NIOConnection {

  public int getTxIsolation();

  public void setTxIsolation(int txIsolation);

  public boolean isAutocommit();

  public void setAutocommit(boolean autocommit);

  public boolean isTxInterrupted();

  public long getLastInsertId();

  public void setLastInsertId(long lastInsertId);

  public Session getSession2();

  public String getSchema();

  public void writeErrMessage(int errno, String msg);

  public void setTxInterrupt(String txInterrputMsg);

  public void routeEndExecuteSQL(String sql, int type, SchemaConfig schema);

  public LoadDataInfileHandler getLoadDataInfileHandler();

  public String getUser();

  public void execute(String sql, int type);

  public void commit();

  public boolean setCharsetIndex(int ci);
}
