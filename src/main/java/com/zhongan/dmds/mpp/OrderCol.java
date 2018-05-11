/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.mpp;

import com.zhongan.dmds.route.ColMeta;

public class OrderCol {

  public final int orderType;
  public final ColMeta colMeta;

  public static final int COL_ORDER_TYPE_ASC = 0; // ASC
  public static final int COL_ORDER_TYPE_DESC = 1; // DESC

  public OrderCol(ColMeta colMeta, int orderType) {
    super();
    this.colMeta = colMeta;
    this.orderType = orderType;
  }

  public int getOrderType() {
    return orderType;
  }

  public ColMeta getColMeta() {
    return colMeta;
  }

}