/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.mysql;

public class BindValue {

  public boolean isNull; /* NULL indicator */
  public boolean isLongData; /* long data indicator */
  public boolean isSet; /* has this parameter been set */

  public long length; /* Default length of data */
  public int type; /* data type */
  public byte scale;

  /**
   * 数据值
   **/
  public byte byteBinding;
  public short shortBinding;
  public int intBinding;
  public float floatBinding;
  public long longBinding;
  public double doubleBinding;
  public Object value; /* Other value to store */

  public void reset() {
    this.isNull = false;
    this.isLongData = false;
    this.isSet = false;

    this.length = 0;
    this.type = 0;
    this.scale = 0;

    this.byteBinding = 0;
    this.shortBinding = 0;
    this.intBinding = 0;
    this.floatBinding = 0;
    this.longBinding = 0L;
    this.doubleBinding = 0D;
    this.value = null;
  }

}