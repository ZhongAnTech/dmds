/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.aggregation.mpp;

import com.zhongan.dmds.mpp.OrderCol;
import com.zhongan.dmds.net.aggregation.RowDataPacketSorter;
import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.util.Comparator;

public class RowDataCmp implements Comparator<RowDataPacket> {

  private OrderCol[] orderCols;

  public RowDataCmp(OrderCol[] orderCols) {
    this.orderCols = orderCols;
  }

  @Override
  public int compare(RowDataPacket o1, RowDataPacket o2) {
    OrderCol[] tmp = this.orderCols;
    int cmp = 0;
    int len = tmp.length;
    // 依次比较order by语句上的多个排序字段的值
    int type = OrderCol.COL_ORDER_TYPE_ASC;
    for (int i = 0; i < len; i++) {
      int colIndex = tmp[i].colMeta.colIndex;
      byte[] left = o1.fieldValues.get(colIndex);
      byte[] right = o2.fieldValues.get(colIndex);
      if (tmp[i].orderType == type) {
        cmp = RowDataPacketSorter.compareObject(left, right, tmp[i]);
      } else {
        cmp = RowDataPacketSorter.compareObject(right, left, tmp[i]);
      }
      if (cmp != 0) {
        return cmp;
      }
    }
    return cmp;
  }

}
