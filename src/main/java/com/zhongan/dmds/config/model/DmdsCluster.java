/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * base on MycatCluster
 */
public final class DmdsCluster {

  private final Map<String, DmdsNode> nodes;
  private final Map<String, List<String>> groups;

  public DmdsCluster(ClusterConfig clusterConf) {
    this.nodes = new HashMap<String, DmdsNode>(clusterConf.getNodes().size());
    this.groups = clusterConf.getGroups();
    for (DmdsNodeConfig conf : clusterConf.getNodes().values()) {
      String name = conf.getName();
      DmdsNode node = new DmdsNode(conf);
      this.nodes.put(name, node);
    }
  }

  public Map<String, DmdsNode> getNodes() {
    return nodes;
  }

  public Map<String, List<String>> getGroups() {
    return groups;
  }

}