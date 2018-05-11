/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.route;

import java.io.Serializable;

public class HavingCols implements Serializable {

  String left;
  String right;
  String operator;
  public ColMeta colMeta;

  public HavingCols(String left, String right, String operator) {
    this.left = left;
    this.right = right;
    this.operator = operator;
  }

  public String getLeft() {
    return left;
  }

  public void setLeft(String left) {
    this.left = left;
  }

  public String getRight() {
    return right;
  }

  public void setRight(String right) {
    this.right = right;
  }

  public String getOperator() {
    return operator;
  }

  public void setOperator(String operator) {
    this.operator = operator;
  }

  public ColMeta getColMeta() {
    return colMeta;
  }

  public void setColMeta(ColMeta colMeta) {
    this.colMeta = colMeta;
  }
}
