/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.aggregation;

import com.zhongan.dmds.commons.util.ByteUtil;
import com.zhongan.dmds.commons.util.CompareUtil;
import com.zhongan.dmds.mpp.OrderCol;
import com.zhongan.dmds.net.protocol.RowDataPacket;
import com.zhongan.dmds.route.ColMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RowDataPacketSorter {

  private static final Logger LOGGER = LoggerFactory.getLogger(RowDataPacketSorter.class);
  protected final OrderCol[] orderCols;

  private Collection<RowDataPacket> sorted = new ConcurrentLinkedQueue<RowDataPacket>();
  private RowDataPacket[] array, resultTemp;
  private int p1, pr, p2;

  public RowDataPacketSorter(OrderCol[] orderCols) {
    super();
    this.orderCols = orderCols;
  }

  public boolean addRow(RowDataPacket row) {
    return this.sorted.add(row);

  }

  public Collection<RowDataPacket> getSortedResult() {
    try {
      this.mergeSort(sorted.toArray(new RowDataPacket[sorted.size()]));
    } catch (Exception e) {
      LOGGER.error("getSortedResultError", e);
    }
    if (array != null) {
      Collections.addAll(this.sorted, array);
    }

    return sorted;
  }

  private RowDataPacket[] mergeSort(RowDataPacket[] result) throws Exception {
    this.sorted.clear();
    array = result;
    if (result == null || result.length < 2 || this.orderCols == null || orderCols.length < 1) {
      return result;
    }
    mergeR(0, result.length - 1);

    return array;
  }

  private void mergeR(int startIndex, int endIndex) {
    if (startIndex < endIndex) {
      int mid = (startIndex + endIndex) / 2;

      mergeR(startIndex, mid);

      mergeR(mid + 1, endIndex);

      merge(startIndex, mid, endIndex);
    }
  }

  private void merge(int startIndex, int midIndex, int endIndex) {
    resultTemp = new RowDataPacket[(endIndex - startIndex + 1)];

    pr = 0;
    p1 = startIndex;
    p2 = midIndex + 1;
    while (p1 <= midIndex || p2 <= endIndex) {
      if (p1 == midIndex + 1) {
        while (p2 <= endIndex) {
          resultTemp[pr++] = array[p2++];

        }
      } else if (p2 == endIndex + 1) {
        while (p1 <= midIndex) {
          resultTemp[pr++] = array[p1++];
        }

      } else {
        compare(0);
      }
    }
    for (p1 = startIndex, p2 = 0; p1 <= endIndex; p1++, p2++) {
      array[p1] = resultTemp[p2];

    }
  }

  /**
   * 递归按照排序字段进行排序
   *
   * @param byColumnIndex
   */
  private void compare(int byColumnIndex) {

    if (byColumnIndex == this.orderCols.length) {
      if (this.orderCols[byColumnIndex - 1].orderType == OrderCol.COL_ORDER_TYPE_ASC) {

        resultTemp[pr++] = array[p1++];
      } else {
        resultTemp[pr++] = array[p2++];
      }
      return;
    }

    byte[] left = array[p1].fieldValues.get(this.orderCols[byColumnIndex].colMeta.colIndex);
    byte[] right = array[p2].fieldValues.get(this.orderCols[byColumnIndex].colMeta.colIndex);

    if (compareObject(left, right, this.orderCols[byColumnIndex]) <= 0) {
      if (compareObject(left, right, this.orderCols[byColumnIndex]) < 0) {
        if (this.orderCols[byColumnIndex].orderType == OrderCol.COL_ORDER_TYPE_ASC) {// 升序
          resultTemp[pr++] = array[p1++];
        } else {
          resultTemp[pr++] = array[p2++];
        }
      } else {// 如果当前字段相等，则按照下一个字段排序
        compare(byColumnIndex + 1);

      }

    } else {
      if (this.orderCols[byColumnIndex].orderType == OrderCol.COL_ORDER_TYPE_ASC) {// 升序
        resultTemp[pr++] = array[p2++];
      } else {
        resultTemp[pr++] = array[p1++];
      }

    }
  }

  public static final int compareObject(Object l, Object r, OrderCol orderCol) {
    return compareObject((byte[]) l, (byte[]) r, orderCol);
  }

  public static final int compareObject(byte[] left, byte[] right, OrderCol orderCol) {
    int colType = orderCol.getColMeta().getColType();
    switch (colType) {
      case ColMeta.COL_TYPE_DECIMAL:
      case ColMeta.COL_TYPE_INT:
      case ColMeta.COL_TYPE_SHORT:
      case ColMeta.COL_TYPE_LONG:
      case ColMeta.COL_TYPE_FLOAT:
      case ColMeta.COL_TYPE_DOUBLE:
      case ColMeta.COL_TYPE_LONGLONG:
      case ColMeta.COL_TYPE_INT24:
      case ColMeta.COL_TYPE_NEWDECIMAL:
        // 因为mysql的日期也是数字字符串方式表达，因此可以跟整数等一起对待
      case ColMeta.COL_TYPE_DATE:
      case ColMeta.COL_TYPE_TIMSTAMP:
      case ColMeta.COL_TYPE_TIME:
      case ColMeta.COL_TYPE_YEAR:
      case ColMeta.COL_TYPE_DATETIME:
      case ColMeta.COL_TYPE_NEWDATE:
      case ColMeta.COL_TYPE_BIT:
        return ByteUtil.compareNumberByte(left, right);
      case ColMeta.COL_TYPE_VAR_STRING:
      case ColMeta.COL_TYPE_STRING:
        // ENUM和SET类型都是字符串，按字符串处理
      case ColMeta.COL_TYPE_ENUM:
      case ColMeta.COL_TYPE_SET:
        return CompareUtil.compareString(ByteUtil.getString(left), ByteUtil.getString(right));

      // BLOB相关类型和GEOMETRY类型不支持排序，略掉
    }
    return 0;
  }
}