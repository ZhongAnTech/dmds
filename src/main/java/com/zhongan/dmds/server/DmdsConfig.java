/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server;

import com.zhongan.dmds.commons.util.TimeUtil;
import com.zhongan.dmds.config.model.*;
import com.zhongan.dmds.core.IDmdsConfig;
import com.zhongan.dmds.core.IPhysicalDBNode;
import com.zhongan.dmds.core.IPhysicalDBPool;
import com.zhongan.dmds.core.NIOConnection;
import com.zhongan.dmds.net.backend.ConfigInitializer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.NetworkChannel;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MycatConfig
 */
public class DmdsConfig implements IDmdsConfig {

  private static final int RELOAD = 1;
  private static final int ROLLBACK = 2;
  private static final int RELOAD_ALL = 3;

  private volatile SystemConfig system;
  private volatile DmdsCluster cluster;
  private volatile DmdsCluster _cluster;
  private volatile QuarantineConfig quarantine;
  private volatile QuarantineConfig _quarantine;
  private volatile Map<String, UserConfig> users;
  private volatile Map<String, UserConfig> _users;
  private volatile Map<String, SchemaConfig> schemas;
  private volatile Map<String, SchemaConfig> _schemas;
  private volatile Map<String, IPhysicalDBNode> dataNodes;
  private volatile Map<String, IPhysicalDBNode> _dataNodes;
  private volatile Map<String, IPhysicalDBPool> dataHosts;
  private volatile Map<String, IPhysicalDBPool> _dataHosts;
  private long reloadTime;
  private long rollbackTime;
  private int status;
  private final ReentrantLock lock;

  public DmdsConfig() {
    ConfigInitializer confInit = new ConfigInitializer(true);
    this.system = confInit.getSystem();
    this.users = confInit.getUsers();
    this.schemas = confInit.getSchemas();
    this.dataHosts = confInit.getDataHosts();

    this.dataNodes = confInit.getDataNodes();
    for (IPhysicalDBPool dbPool : dataHosts.values()) {
      dbPool.setSchemas(getDataNodeSchemasOfDataHost(dbPool.getHostName()));
    }
    this.quarantine = confInit.getQuarantine();
    this.cluster = confInit.getCluster();

    this.reloadTime = TimeUtil.currentTimeMillis();
    this.rollbackTime = -1L;
    this.status = RELOAD;
    this.lock = new ReentrantLock();
  }

  @Override
  public SystemConfig getSystem() {
    return system;
  }

  @Override
  public void setSocketParams(NIOConnection con, boolean isFrontChannel) throws IOException {
    int sorcvbuf = 0;
    int sosndbuf = 0;
    int soNoDelay = 0;
    if (isFrontChannel) {
      sorcvbuf = system.getFrontsocketsorcvbuf();
      sosndbuf = system.getFrontsocketsosndbuf();
      soNoDelay = system.getFrontSocketNoDelay();
    } else {
      sorcvbuf = system.getBacksocketsorcvbuf();
      sosndbuf = system.getBacksocketsosndbuf();
      soNoDelay = system.getBackSocketNoDelay();
    }
    NetworkChannel channel = con.getChannel();
    channel.setOption(StandardSocketOptions.SO_RCVBUF, sorcvbuf);
    channel.setOption(StandardSocketOptions.SO_SNDBUF, sosndbuf);
    channel.setOption(StandardSocketOptions.TCP_NODELAY, soNoDelay == 1);
    channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

    con.setMaxPacketSize(system.getMaxPacketSize());
    con.setPacketHeaderSize(system.getPacketHeaderSize());
    con.setIdleTimeout(system.getIdleTimeout());
    con.setCharset(system.getCharset());

  }

  @Override
  public Map<String, UserConfig> getUsers() {
    return users;
  }

  @Override
  public Map<String, UserConfig> getBackupUsers() {
    return _users;
  }

  @Override
  public Map<String, SchemaConfig> getSchemas() {
    return schemas;
  }

  @Override
  public Map<String, SchemaConfig> getBackupSchemas() {
    return _schemas;
  }

  @Override
  public Map<String, IPhysicalDBNode> getDataNodes() {
    return dataNodes;
  }

  @Override
  public void setDataNodes(Map<String, IPhysicalDBNode> map) {
    this.dataNodes = map;
  }

  @Override
  public String[] getDataNodeSchemasOfDataHost(String dataHost) {
    ArrayList<String> schemas = new ArrayList<String>(30);
    for (IPhysicalDBNode dn : dataNodes.values()) {
      if (dn.getDbPool().getHostName().equals(dataHost)) {
        schemas.add(dn.getDatabase());
      }
    }
    return schemas.toArray(new String[schemas.size()]);
  }

  @Override
  public Map<String, IPhysicalDBNode> getBackupDataNodes() {
    return _dataNodes;
  }

  @Override
  public Map<String, IPhysicalDBPool> getDataHosts() {
    return dataHosts;
  }

  @Override
  public Map<String, IPhysicalDBPool> getBackupDataHosts() {
    return _dataHosts;
  }

  @Override
  public DmdsCluster getCluster() {
    return cluster;
  }

  @Override
  public DmdsCluster getBackupCluster() {
    return _cluster;
  }

  @Override
  public QuarantineConfig getQuarantine() {
    return quarantine;
  }

  @Override
  public QuarantineConfig getBackupQuarantine() {
    return _quarantine;
  }

  @Override
  public ReentrantLock getLock() {
    return lock;
  }

  @Override
  public long getReloadTime() {
    return reloadTime;
  }

  @Override
  public long getRollbackTime() {
    return rollbackTime;
  }

  @Override
  public void reload(Map<String, UserConfig> users, Map<String, SchemaConfig> schemas,
      Map<String, IPhysicalDBNode> dataNodes, Map<String, IPhysicalDBPool> dataHosts,
      DmdsCluster cluster,
      QuarantineConfig quarantine, boolean reloadAll) {
    apply(users, schemas, dataNodes, dataHosts, cluster, quarantine, reloadAll);
    this.reloadTime = TimeUtil.currentTimeMillis();
    this.status = reloadAll ? RELOAD_ALL : RELOAD;
  }

  @Override
  public boolean canRollback() {
    if (_users == null || _schemas == null || _dataNodes == null || _dataHosts == null
        || _cluster == null
        || _quarantine == null || status == ROLLBACK) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public void rollback(Map<String, UserConfig> users, Map<String, SchemaConfig> schemas,
      Map<String, IPhysicalDBNode> dataNodes, Map<String, IPhysicalDBPool> dataHosts,
      DmdsCluster cluster,
      QuarantineConfig quarantine) {
    apply(users, schemas, dataNodes, dataHosts, cluster, quarantine, status == RELOAD_ALL);
    this.rollbackTime = TimeUtil.currentTimeMillis();
    this.status = ROLLBACK;
  }

  private void apply(Map<String, UserConfig> users, Map<String, SchemaConfig> schemas,
      Map<String, IPhysicalDBNode> dataNodes, Map<String, IPhysicalDBPool> dataHosts,
      DmdsCluster cluster,
      QuarantineConfig quarantine, boolean isLoadAll) {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (isLoadAll) {
        // stop datasource heartbeat
        Map<String, IPhysicalDBPool> oldDataHosts = this.dataHosts;
        if (oldDataHosts != null) {
          for (IPhysicalDBPool n : oldDataHosts.values()) {
            if (n != null) {
              n.stopHeartbeat();
            }
          }
        }
        this._dataNodes = this.dataNodes;
        this._dataHosts = this.dataHosts;
      }
      this._users = this.users;
      this._schemas = this.schemas;

      this._cluster = this.cluster;
      this._quarantine = this.quarantine;

      if (isLoadAll) {
        // start datasoruce heartbeat

        if (dataNodes != null) {
          for (IPhysicalDBPool n : dataHosts.values()) {
            if (n != null) {
              n.startHeartbeat();
            }
          }
        }
        this.dataNodes = dataNodes;
        this.dataHosts = dataHosts;
      }
      this.users = users;
      this.schemas = schemas;
      this.cluster = cluster;
      this.quarantine = quarantine;
    } finally {
      lock.unlock();
    }
  }

}
