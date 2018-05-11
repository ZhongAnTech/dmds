/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model.rule;

public class TableRuleConfig {

  private final String name;
  private final RuleConfig rule;

  public TableRuleConfig(String name, RuleConfig rule) {
    if (name == null) {
      throw new IllegalArgumentException("name is null");
    }
    this.name = name;
    if (rule == null) {
      throw new IllegalArgumentException("no rule is found");
    }
    this.rule = rule;
  }

  public String getName() {
    return name;
  }

  /**
   * @return unmodifiable
   */
  public RuleConfig getRule() {
    return rule;
  }

}