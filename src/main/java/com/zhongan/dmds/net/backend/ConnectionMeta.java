/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.backend;

import com.zhongan.dmds.core.BackendConnection;

/**
 * connection metadata info
 */
public class ConnectionMeta {

  private final String schema;
  private final String charset;
  private final int txIsolation;
  private final boolean autocommit;

  public ConnectionMeta(String schema, String charset, int txIsolation, boolean autocommit) {
    super();
    this.schema = schema;
    this.charset = charset;
    this.txIsolation = txIsolation;
    this.autocommit = autocommit;
  }

  public String getSchema() {
    return schema;
  }

  // public String getCharset() {
  // return charset;
  // }
  //
  // public int getTxIsolation() {
  // return txIsolation;
  // }
  //
  // public boolean isAutocommit() {
  // return autocommit;
  // }

  public boolean isSameSchema(BackendConnection theCon) {
    return theCon.getSchema().equals(schema);
  }

  /**
   * get metadata similarity
   *
   * @param theCon
   * @return
   */
  public int getMetaSimilarity(BackendConnection theCon) {
    int result = 0;
    if (schema == null || schema.equals(theCon.getSchema())) {
      result++;
    }
    if (charset == null || charset.equals(theCon.getCharset())) {
      result++;
    }
    if (txIsolation == -1 || txIsolation == theCon.getTxIsolation()) {
      result++;
    }
    if (autocommit == theCon.isAutocommit()) {
      result++;
    }
    return result;
  }

  @Override
  public String toString() {
    return "ConnectionMeta [schema=" + schema + ", charset=" + charset + ", txIsolation="
        + txIsolation
        + ", autocommit=" + autocommit + "]";
  }

}