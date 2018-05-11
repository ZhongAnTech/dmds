/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.aggregation;

import com.zhongan.dmds.mpp.OrderCol;
import com.zhongan.dmds.net.aggregation.mpp.RowDataSorter;
import com.zhongan.dmds.net.protocol.RowDataPacket;

public class RangRowDataPacketSorter extends RowDataSorter {

  public RangRowDataPacketSorter(OrderCol[] orderCols) {
    super(orderCols);
  }

  public boolean ascDesc(int byColumnIndex) {
    if (this.orderCols[byColumnIndex].orderType == OrderCol.COL_ORDER_TYPE_ASC) {// 升序
      return true;
    }
    return false;
  }

  public int compareRowData(RowDataPacket l, RowDataPacket r, int byColumnIndex) {
    byte[] left = l.fieldValues.get(this.orderCols[byColumnIndex].colMeta.colIndex);
    byte[] right = r.fieldValues.get(this.orderCols[byColumnIndex].colMeta.colIndex);

    return compareObject(left, right, this.orderCols[byColumnIndex]);
  }
}