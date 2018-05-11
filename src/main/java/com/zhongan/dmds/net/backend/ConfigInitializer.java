/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.backend;

import com.zhongan.dmds.config.loader.ConfigLoader;
import com.zhongan.dmds.config.loader.SchemaLoader;
import com.zhongan.dmds.config.loader.xml.XMLConfigLoader;
import com.zhongan.dmds.config.loader.xml.XMLSchemaLoader;
import com.zhongan.dmds.config.model.*;
import com.zhongan.dmds.core.IPhysicalDBNode;
import com.zhongan.dmds.core.IPhysicalDBPool;
import com.zhongan.dmds.core.IPhysicalDatasource;
import com.zhongan.dmds.exception.ConfigException;
import com.zhongan.dmds.net.jdbc.JDBCDatasource;
import com.zhongan.dmds.net.mysql.nio.MySQLDataSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 2017.2 Remove support for sequence handler
 */
public class ConfigInitializer {

  private volatile SystemConfig system;
  private volatile DmdsCluster cluster;
  private volatile QuarantineConfig quarantine;
  private volatile Map<String, UserConfig> users;
  private volatile Map<String, SchemaConfig> schemas;
  private volatile Map<String, IPhysicalDBNode> dataNodes;
  private volatile Map<String, IPhysicalDBPool> dataHosts;

  public ConfigInitializer(boolean loadDataHost) {
    SchemaLoader schemaLoader = new XMLSchemaLoader();
    XMLConfigLoader configLoader = new XMLConfigLoader(schemaLoader);
    schemaLoader = null;
    this.system = configLoader.getSystemConfig();
    this.users = configLoader.getUserConfigs();
    this.schemas = configLoader.getSchemaConfigs();
    if (loadDataHost) {
      this.dataHosts = initDataHosts(configLoader);
      this.dataNodes = initDataNodes(configLoader);
    }
    this.quarantine = configLoader.getQuarantineConfig();
    this.cluster = initCobarCluster(configLoader);

    this.checkConfig();
  }

  private void checkConfig() throws ConfigException {
    if (users == null || users.isEmpty()) {
      return;
    }
    for (UserConfig uc : users.values()) {
      if (uc == null) {
        continue;
      }
      Set<String> authSchemas = uc.getSchemas();
      if (authSchemas == null) {
        continue;
      }
      for (String schema : authSchemas) {
        if (!schemas.containsKey(schema)) {
          String errMsg =
              "schema " + schema + " refered by user " + uc.getName() + " is not exist!";
          throw new ConfigException(errMsg);
        }
      }
    }

    for (SchemaConfig sc : schemas.values()) {
      if (null == sc) {
        continue;
      }
    }
  }

  public SystemConfig getSystem() {
    return system;
  }

  public DmdsCluster getCluster() {
    return cluster;
  }

  public QuarantineConfig getQuarantine() {
    return quarantine;
  }

  public Map<String, UserConfig> getUsers() {
    return users;
  }

  public Map<String, SchemaConfig> getSchemas() {
    return schemas;
  }

  public Map<String, IPhysicalDBNode> getDataNodes() {
    return dataNodes;
  }

  public Map<String, IPhysicalDBPool> getDataHosts() {
    return this.dataHosts;
  }

  private DmdsCluster initCobarCluster(ConfigLoader configLoader) {
    return new DmdsCluster(configLoader.getClusterConfig());
  }

  private Map<String, IPhysicalDBPool> initDataHosts(ConfigLoader configLoader) {
    Map<String, DataHostConfig> nodeConfs = configLoader.getDataHosts();
    Map<String, IPhysicalDBPool> nodes = new HashMap<String, IPhysicalDBPool>(nodeConfs.size());
    for (DataHostConfig conf : nodeConfs.values()) {
      PhysicalDBPool pool = getPhysicalDBPool(conf, configLoader);
      nodes.put(pool.getHostName(), pool);
    }
    return nodes;
  }

  private PhysicalDatasource[] createDataSource(DataHostConfig conf, String hostName, String dbType,
      String dbDriver,
      DBHostConfig[] nodes, boolean isRead) {
    PhysicalDatasource[] dataSources = new PhysicalDatasource[nodes.length];
    if (dbType.equals("mysql") && dbDriver.equals("native")) {
      for (int i = 0; i < nodes.length; i++) {
        nodes[i].setIdleTimeout(system.getIdleTimeout());
        MySQLDataSource ds = new MySQLDataSource(nodes[i], conf, isRead);
        dataSources[i] = ds;
      }

    } else if (dbDriver.equals("jdbc")) {
      for (int i = 0; i < nodes.length; i++) {
        nodes[i].setIdleTimeout(system.getIdleTimeout());
        JDBCDatasource ds = new JDBCDatasource(nodes[i], conf, isRead);
        dataSources[i] = ds;
      }
    } else {
      throw new ConfigException("not supported yet !" + hostName);
    }
    return dataSources;
  }

  private PhysicalDBPool getPhysicalDBPool(DataHostConfig conf, ConfigLoader configLoader) {
    String name = conf.getName();
    String dbType = conf.getDbType();
    String dbDriver = conf.getDbDriver();
    IPhysicalDatasource[] writeSources = createDataSource(conf, name, dbType, dbDriver,
        conf.getWriteHosts(),
        false);
    Map<Integer, DBHostConfig[]> readHostsMap = conf.getReadHosts();
    Map<Integer, IPhysicalDatasource[]> readSourcesMap = new HashMap<Integer, IPhysicalDatasource[]>(
        readHostsMap.size());
    for (Map.Entry<Integer, DBHostConfig[]> entry : readHostsMap.entrySet()) {
      PhysicalDatasource[] readSources = createDataSource(conf, name, dbType, dbDriver,
          entry.getValue(), true);
      readSourcesMap.put(entry.getKey(), readSources);
    }
    PhysicalDBPool pool = new PhysicalDBPool(conf.getName(), conf, writeSources, readSourcesMap,
        conf.getBalance(),
        conf.getWriteType());
    return pool;
  }

  private Map<String, IPhysicalDBNode> initDataNodes(ConfigLoader configLoader) {
    Map<String, DataNodeConfig> nodeConfs = configLoader.getDataNodes();
    Map<String, IPhysicalDBNode> nodes = new HashMap<String, IPhysicalDBNode>(nodeConfs.size());
    for (DataNodeConfig conf : nodeConfs.values()) {
      IPhysicalDBPool pool = this.dataHosts.get(conf.getDataHost());
      if (pool == null) {
        throw new ConfigException("dataHost not exists " + conf.getDataHost());

      }
      PhysicalDBNode dataNode = new PhysicalDBNode(conf.getName(), conf.getDatabase(), pool);
      nodes.put(dataNode.getName(), dataNode);
    }
    return nodes;
  }

}