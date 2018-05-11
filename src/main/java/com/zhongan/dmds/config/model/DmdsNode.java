/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model;

/**
 * Base on MycatNode
 */
public class DmdsNode {

  private final String name;
  private final DmdsNodeConfig config;

  public DmdsNode(DmdsNodeConfig config) {
    this.name = config.getName();
    this.config = config;
  }

  public String getName() {
    return name;
  }

  public DmdsNodeConfig getConfig() {
    return config;
  }

  public boolean isOnline() {
    return (true);
  }

}