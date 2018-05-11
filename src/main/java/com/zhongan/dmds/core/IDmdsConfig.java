/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.zhongan.dmds.config.model.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Extract from MycatConfig
 */
public interface IDmdsConfig {

  SystemConfig getSystem();

  void setSocketParams(NIOConnection con, boolean isFrontChannel) throws IOException;

  Map<String, UserConfig> getUsers();

  Map<String, UserConfig> getBackupUsers();

  Map<String, SchemaConfig> getSchemas();

  Map<String, SchemaConfig> getBackupSchemas();

  Map<String, IPhysicalDBNode> getDataNodes();

  void setDataNodes(Map<String, IPhysicalDBNode> map);

  String[] getDataNodeSchemasOfDataHost(String dataHost);

  Map<String, IPhysicalDBNode> getBackupDataNodes();

  Map<String, IPhysicalDBPool> getDataHosts();

  Map<String, IPhysicalDBPool> getBackupDataHosts();

  DmdsCluster getCluster();

  DmdsCluster getBackupCluster();

  QuarantineConfig getQuarantine();

  QuarantineConfig getBackupQuarantine();

  ReentrantLock getLock();

  long getReloadTime();

  long getRollbackTime();

  void reload(Map<String, UserConfig> users, Map<String, SchemaConfig> schemas,
      Map<String, IPhysicalDBNode> dataNodes, Map<String, IPhysicalDBPool> dataHosts,
      DmdsCluster cluster,
      QuarantineConfig quarantine, boolean reloadAll);

  boolean canRollback();

  void rollback(Map<String, UserConfig> users, Map<String, SchemaConfig> schemas,
      Map<String, IPhysicalDBNode> dataNodes, Map<String, IPhysicalDBPool> dataHosts,
      DmdsCluster cluster,
      QuarantineConfig quarantine);

}
