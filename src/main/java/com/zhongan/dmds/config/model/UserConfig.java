/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model;

import java.util.Set;

public class UserConfig {

  private String name;
  private String password;// 明文
  private String encryptPassword; // 密文
  private int benchmark = 0; // 负载限制, 默认0表示不限制
  private String benchmarkSmsTel;

  private boolean readOnly = false;

  public boolean isReadOnly() {
    return readOnly;
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  private Set<String> schemas;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public int getBenchmark() {
    return benchmark;
  }

  public void setBenchmark(int benchmark) {
    this.benchmark = benchmark;
  }

  public String getBenchmarkSmsTel() {
    return benchmarkSmsTel;
  }

  public void setBenchmarkSmsTel(String benchmarkSmsTel) {
    this.benchmarkSmsTel = benchmarkSmsTel;
  }

  public Set<String> getSchemas() {
    return schemas;
  }

  public String getEncryptPassword() {
    return this.encryptPassword;
  }

  public void setEncryptPassword(String encryptPassword) {
    this.encryptPassword = encryptPassword;
  }

  public void setSchemas(Set<String> schemas) {
    this.schemas = schemas;
  }

  @Override
  public String toString() {
    return "UserConfig [name=" + this.name + ", password=" + this.password + ", encryptPassword="
        + this.encryptPassword + ", benchmark=" + this.benchmark + ", benchmarkSmsTel="
        + this.benchmarkSmsTel
        + ", readOnly=" + this.readOnly + ", schemas=" + this.schemas + "]";
  }

}