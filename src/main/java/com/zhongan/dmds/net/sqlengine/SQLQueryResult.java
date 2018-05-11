/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.sqlengine;

public class SQLQueryResult<T> {

  private final T result;
  private final boolean success;

  public SQLQueryResult(T result, boolean success) {
    super();
    this.result = result;
    this.success = success;
  }

  public T getResult() {
    return result;
  }

  public boolean isSuccess() {
    return success;
  }

}
