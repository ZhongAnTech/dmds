/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.statistic;

public final class SQLRecord implements Comparable<SQLRecord> {

  public String host;
  public String schema;
  public String statement;
  public long startTime;
  public long executeTime;
  public String dataNode;
  public int dataNodeIndex;

  @Override
  public int compareTo(SQLRecord o) {
    return (int) (executeTime - o.executeTime);
  }

  @Override
  public boolean equals(Object arg0) {
    return super.equals(arg0);
  }

  @Override
  public int hashCode() {
    // TODO Auto-generated method stub
    return super.hashCode();
  }

}