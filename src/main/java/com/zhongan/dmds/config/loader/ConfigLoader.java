/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.loader;

import com.zhongan.dmds.config.model.*;

import java.util.Map;

public interface ConfigLoader {

  SchemaConfig getSchemaConfig(String schema);

  Map<String, SchemaConfig> getSchemaConfigs();

  Map<String, DataNodeConfig> getDataNodes();

  Map<String, DataHostConfig> getDataHosts();

  SystemConfig getSystemConfig();

  UserConfig getUserConfig(String user);

  Map<String, UserConfig> getUserConfigs();

  QuarantineConfig getQuarantineConfig();

  ClusterConfig getClusterConfig();
}