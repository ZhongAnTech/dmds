/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route.function;

import com.zhongan.dmds.config.model.rule.RuleAlgorithm;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * number column partion by Mod operator if count is 10 then 0 to 0,21 to 1 (21 % 10 =1)
 */
public class PartitionByMod extends AbstractPartitionAlgorithm implements RuleAlgorithm {

  private int count;

  @Override
  public void init() {

  }

  public void setCount(int count) {
    this.count = count;
  }

  @Override
  public Integer calculate(String columnValue) {
    columnValue = NumberParseUtil.eliminateQoute(columnValue);
    BigInteger bigNum = new BigInteger(columnValue).abs();
    return (bigNum.mod(BigInteger.valueOf(count))).intValue();
  }

  private static void hashTest() {
    PartitionByMod hash = new PartitionByMod();
    hash.setCount(11);
    hash.init();

    int[] bucket = new int[hash.count];

    Map<Integer, List<Integer>> hashed = new HashMap<>();

    int total = 1000_0000;// 数据量
    int c = 0;
    for (int i = 100_0000; i < total + 100_0000; i++) {// 假设分片键从100万开始
      c++;
      int h = hash.calculate(Integer.toString(i));
      bucket[h]++;
      List<Integer> list = hashed.get(h);
      if (list == null) {
        list = new ArrayList<>();
        hashed.put(h, list);
      }
      list.add(i);
    }
    System.out.println(c + "   " + total);
    double d = 0;
    c = 0;
    int idx = 0;
    System.out.println("index    bucket   ratio");
    for (int i : bucket) {
      d += i / (double) total;
      c += i;
      System.out.println(idx++ + "  " + i + "   " + (i / (double) total));
    }
    System.out.println(d + "  " + c);

    System.out.println("****************************************************");
    rehashTest(hashed.get(0));
  }

  private static void rehashTest(List<Integer> partition) {
    PartitionByMod hash = new PartitionByMod();
    hash.count = 110;// 分片数
    hash.init();

    int[] bucket = new int[hash.count];

    int total = partition.size();// 数据量
    int c = 0;
    for (int i : partition) {// 假设分片键从100万开始
      c++;
      int h = hash.calculate(Integer.toString(i));
      bucket[h]++;
    }
    System.out.println(c + "   " + total);
    c = 0;
    int idx = 0;
    System.out.println("index    bucket   ratio");
    for (int i : bucket) {
      c += i;
      System.out.println(idx++ + "  " + i + "   " + (i / (double) total));
    }
  }

  public static void main(String[] args) {
    hashTest();
  }
}