/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.loader.xml;

import com.zhongan.dmds.config.loader.ConfigLoader;
import com.zhongan.dmds.config.loader.SchemaLoader;
import com.zhongan.dmds.config.model.*;

import java.util.Map;

public class XMLConfigLoader implements ConfigLoader {

  /**
   * unmodifiable
   */
  private final Map<String, DataHostConfig> dataHosts;
  /**
   * unmodifiable
   */
  private final Map<String, DataNodeConfig> dataNodes;
  /**
   * unmodifiable
   */
  private final Map<String, SchemaConfig> schemas;
  private final SystemConfig system;
  /**
   * unmodifiable
   */
  private final Map<String, UserConfig> users;
  private final QuarantineConfig quarantine;
  private final ClusterConfig cluster;

  public XMLConfigLoader(SchemaLoader schemaLoader) {
    XMLServerLoader serverLoader = new XMLServerLoader();
    this.system = serverLoader.getSystem();
    this.users = serverLoader.getUsers();
    this.quarantine = serverLoader.getQuarantine();
    this.cluster = serverLoader.getCluster();
    this.dataHosts = schemaLoader.getDataHosts();
    this.dataNodes = schemaLoader.getDataNodes();
    this.schemas = schemaLoader.getSchemas();
    schemaLoader = null;
  }

  @Override
  public ClusterConfig getClusterConfig() {
    return cluster;
  }

  @Override
  public QuarantineConfig getQuarantineConfig() {
    return quarantine;
  }

  @Override
  public UserConfig getUserConfig(String user) {
    return users.get(user);
  }

  @Override
  public Map<String, UserConfig> getUserConfigs() {
    return users;
  }

  @Override
  public SystemConfig getSystemConfig() {
    return system;
  }

  @Override
  public Map<String, SchemaConfig> getSchemaConfigs() {
    return schemas;
  }

  @Override
  public Map<String, DataNodeConfig> getDataNodes() {
    return dataNodes;
  }

  @Override
  public Map<String, DataHostConfig> getDataHosts() {
    return dataHosts;
  }

  @Override
  public SchemaConfig getSchemaConfig(String schema) {
    return schemas.get(schema);
  }

}