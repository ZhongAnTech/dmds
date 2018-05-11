/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model.rule;

/**
 * 2016.12 dmds增加分表功能
 */
public interface RuleAlgorithm {

  /**
   * init
   *
   * @param
   */
  void init();

  /**
   * 计算分库 return sharding nodes's id columnValue is column's value
   *
   * @return never null
   */
  Integer calculate(String columnValue);

  /**
   * 通过db的下标语句计算分库
   *
   * @param tableIndex
   * @return
   */
  public Integer calculateForDBIndex(Integer dbIndex);

  /**
   * 计算分表，如果不需要分表请返回null
   *
   * @param columnValue
   * @return
   */
  Integer calculateTable(String columnValue);

  /**
   * 通过表的下标语句计算分表， 如果不需要分表请返回null
   *
   * @param tableIndex
   * @return
   */
  Integer calculateTableForTableIndex(Integer tableIndex);

  /**
   * 计算分库区间
   *
   * @param beginValue
   * @param endValue
   * @return
   */
  Integer[] calculateRange(String beginValue, String endValue);

  /**
   * 分库数目,如果不分库，返回null
   *
   * @return
   */
  public Integer getDbCount();

  /**
   * 分表数目，如果不分表，返回null
   *
   * @return
   */
  public Integer getTbCount();
}