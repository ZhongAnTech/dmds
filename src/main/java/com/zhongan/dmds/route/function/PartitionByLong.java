/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route.function;

import com.zhongan.dmds.commons.route.PartitionUtil;
import com.zhongan.dmds.config.model.rule.RuleAlgorithm;

public final class PartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm {

  protected int[] count;
  protected int[] length;
  protected PartitionUtil partitionUtil;

  private static int[] toIntArray(String string) {
    String[] strs = com.zhongan.dmds.commons.util.SplitUtil.split(string, ',', true);
    int[] ints = new int[strs.length];
    for (int i = 0; i < strs.length; ++i) {
      ints[i] = Integer.parseInt(strs[i]);
    }
    return ints;
  }

  public void setPartitionCount(String partitionCount) {
    this.count = toIntArray(partitionCount);
  }

  public void setPartitionLength(String partitionLength) {
    this.length = toIntArray(partitionLength);
  }

  @Override
  public void init() {
    partitionUtil = new PartitionUtil(count, length);

  }

  @Override
  public Integer calculate(String columnValue) {
    columnValue = NumberParseUtil.eliminateQoute(columnValue);
    long key = Long.parseLong(columnValue);
    return partitionUtil.partition(key);
  }

  @Override
  public Integer[] calculateRange(String beginValue, String endValue) {
    return AbstractPartitionAlgorithm.calculateSequenceRange(this, beginValue, endValue);
  }

}