/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route.function;

import java.math.BigInteger;

/**
 * Base on PartitionByMod and PartitionByHashMod
 * 分库分表 单库中支持同结构多表 例如： db_00: tb_0000,tb_0001,tb_0002 db_01: tb_0003,tb_0004,tb_0005
 */
public class PartitionTbByMod extends AbstractPartitionAlgorithm {

  /**
   * 数据库节点数据量
   */
  private Integer dbCount;

  /**
   * 单库里面的数据表数量 <1或者null 表示不分表
   */
  private Integer tbCount = 0;

  /**
   * 分片字段类型,0-数值，1-字符串
   */
  private Integer shardColumnType = 0;

  /**
   * 已经废弃
   *
   * @param columnValue 分库分表字段列值
   */
  @Override
  public Integer calculate(String columnValue) {
    columnValue = NumberParseUtil.eliminateQoute(columnValue);

    if (shardColumnType == 1) {
      columnValue = String.valueOf(columnValue.hashCode());
    }
    BigInteger bigNum = new BigInteger(columnValue).abs();
    return (bigNum.mod(BigInteger.valueOf(this.dbCount))).intValue();
  }

  /**
   * 根据分库分表字段计算所在表
   *
   * @param columnValue 分库分表字段列值
   */
  public Integer calculateTable(String columnValue) {
    if (this.tbCount == null) {
      return null;
    } else if (this.tbCount < 1) {
      return null;
    }
    columnValue = NumberParseUtil.eliminateQoute(columnValue);

    if (shardColumnType == 1) {
      columnValue = String.valueOf(columnValue.hashCode());
    }
    BigInteger intColValue = new BigInteger(columnValue).abs();
    return (intColValue.mod(BigInteger.valueOf(tbCount * dbCount))).intValue();
  }

  /**
   * 计算表所在库下表
   *
   * @param tbIndex 计算好的表下标（calculateTableForTableIndex） 例如：2库6表，当tbIndex=2时，库下标是：2/3(每个库中的表数量)=0
   * @return
   */
  public Integer calculateForDBIndex(Integer tbIndex) {
    BigInteger bigNum = new BigInteger(String.valueOf(tbIndex)).abs();
    return bigNum.divide(BigInteger.valueOf(this.tbCount)).intValue();
  }

  /**
   * 计算表下标， 如果不需要分表请返回null
   *
   * @param tableIndex 表索引 例如：1024张表(0 <= tableIndex < 1024)
   * @return
   */
  public Integer calculateTableForTableIndex(Integer tableIndex) {
    if (this.tbCount == null) {
      return null;
    } else if (this.tbCount < 1) {
      return null;
    }
    BigInteger intColValue = new BigInteger(String.valueOf(tableIndex)).abs();
    return (intColValue.mod(BigInteger.valueOf(tbCount * dbCount))).intValue();
  }

  public void setDbCount(Integer dbCount) {
    this.dbCount = dbCount;
  }

  public void setTbCount(Integer tbCount) {
    this.tbCount = tbCount;
  }

  public Integer getDbCount() {
    return dbCount;
  }

  public Integer getTbCount() {
    return tbCount;
  }

  public Integer getShardColumnType() {
    return shardColumnType;
  }

  public void setShardColumnType(Integer shardColumnType) {
    this.shardColumnType = shardColumnType;
  }

}
