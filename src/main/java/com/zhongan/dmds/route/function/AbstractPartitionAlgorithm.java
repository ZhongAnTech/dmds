/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route.function;

import com.zhongan.dmds.config.model.rule.RuleAlgorithm;

/**
 * 路由分片函数抽象类 为了实现一个默认的支持范围分片的函数 calcualteRange 重写它以实现自己的范围路由规则
 * @author lxy
 * 2016.12 DMDS支持分表功能
 */
public abstract class AbstractPartitionAlgorithm implements RuleAlgorithm {

  @Override
  public void init() {
  }

  /**
   * 返回所有被路由到的节点的编号 返回长度为0的数组表示所有节点都被路由（默认） 返回null表示没有节点被路由到
   */
  @Override
  public Integer[] calculateRange(String beginValue, String endValue) {
    return new Integer[0];
  }

  public Integer calculateForDBIndex(Integer dbIndex) {
    return null;
  }

  public Integer calculateTable(String columnValue) {
    return null;
  }

  public Integer calculateTableForTableIndex(Integer tableIndex) {
    return null;
  }

  /**
   * 对于存储数据按顺序存放的字段做范围路由，可以使用这个函数
   *
   * @param algorithm
   * @param beginValue
   * @param endValue
   * @return
   */
  public static Integer[] calculateSequenceRange(AbstractPartitionAlgorithm algorithm,
      String beginValue,
      String endValue) {
    Integer begin = 0, end = 0;

    Integer tableIndexBegin = algorithm.calculateTable(beginValue);
    Integer tableIndexEnd = algorithm.calculateTable(endValue);

    begin = algorithm.calculateForDBIndex(tableIndexBegin);
    end = algorithm.calculateForDBIndex(tableIndexEnd);

    if (begin == null || end == null) {
      return new Integer[0];
    }

    if (end >= begin) {
      int len = end - begin + 1;
      Integer[] re = new Integer[len];

      for (int i = 0; i < len; i++) {
        re[i] = begin + i;
      }

      return re;
    } else {
      return new Integer[0];
    }
  }

  @Override
  public Integer getDbCount() {
    return 1;
  }

  @Override
  public Integer getTbCount() {
    return 1;
  }

}
