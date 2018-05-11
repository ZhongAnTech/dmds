/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model.rule;

/**
 * 分片规则，column是用于分片的数据库物理字段
 */
public class RuleConfig {

  private final String column;
  private final String functionName;
  private RuleAlgorithm ruleAlgorithm;

  public RuleConfig(String column, String functionName) {
    if (functionName == null) {
      throw new IllegalArgumentException("functionName is null");
    }
    this.functionName = functionName;
    if (column == null || column.length() <= 0) {
      throw new IllegalArgumentException("no rule column is found");
    }
    this.column = column;
  }

  public RuleAlgorithm getRuleAlgorithm() {
    return ruleAlgorithm;
  }

  public void setRuleAlgorithm(RuleAlgorithm ruleAlgorithm) {
    this.ruleAlgorithm = ruleAlgorithm;
  }

  /**
   * @return unmodifiable, upper-case
   */
  public String getColumn() {
    return column;
  }

  public String getFunctionName() {
    return functionName;
  }

}