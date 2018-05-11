/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.aggregation.mpp;

import com.zhongan.dmds.net.protocol.RowDataPacket;

import java.util.List;

public interface HeapItf {

  /**
   * 构建堆
   */
  void buildHeap();

  /**
   * 获取堆根节点
   *
   * @return
   */
  RowDataPacket getRoot();

  /**
   * 向堆添加元素
   *
   * @param row
   */
  void add(RowDataPacket row);

  /**
   * 获取堆数据
   *
   * @return
   */
  List<RowDataPacket> getData();

  /**
   * 设置根节点元素
   *
   * @param root
   */
  void setRoot(RowDataPacket root);

  /**
   * 向已满的堆添加元素
   *
   * @param row
   */
  boolean addIfRequired(RowDataPacket row);

  /**
   * 堆排序
   */
  void heapSort(int size);

}
