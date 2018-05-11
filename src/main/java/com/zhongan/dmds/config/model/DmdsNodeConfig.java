/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model;

/**
 * Base on MycatNodeConfig
 */
public final class DmdsNodeConfig {

  private String name;
  private String host;
  private int port;
  private int weight;

  public DmdsNodeConfig(String name, String host, int port, int weight) {
    this.name = name;
    this.host = host;
    this.port = port;
    this.weight = weight;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getWeight() {
    return weight;
  }

  public void setWeight(int weight) {
    this.weight = weight;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("[name=").append(name).append(",host=").append(host)
        .append(",port=")
        .append(port).append(",weight=").append(weight).append(']').toString();
  }

}