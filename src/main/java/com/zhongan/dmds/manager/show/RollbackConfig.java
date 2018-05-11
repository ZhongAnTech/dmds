/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.show;

import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.config.model.DmdsCluster;
import com.zhongan.dmds.config.model.QuarantineConfig;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.UserConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IDmdsConfig;
import com.zhongan.dmds.core.IPhysicalDBNode;
import com.zhongan.dmds.core.IPhysicalDBPool;
import com.zhongan.dmds.manager.ManagerConnection;
import com.zhongan.dmds.net.protocol.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 移除无用的Catch
 */
public final class RollbackConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(RollbackConfig.class);

  public static void execute(ManagerConnection c) {
    final ReentrantLock lock = DmdsContext.getInstance().getConfig().getLock();
    lock.lock();
    try {
      if (rollback()) {
        StringBuilder s = new StringBuilder();
        s.append(c).append("Rollback config success by manager");
        LOGGER.warn(s.toString());
        OkPacket ok = new OkPacket();
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
        ok.message = "Rollback config success".getBytes();
        ok.write(c);
      } else {
        c.writeErrMessage(ErrorCode.ER_YES, "Rollback config failure");
      }
    } finally {
      lock.unlock();
    }
  }

  private static boolean rollback() {
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    Map<String, UserConfig> users = conf.getBackupUsers();
    Map<String, SchemaConfig> schemas = conf.getBackupSchemas();
    Map<String, IPhysicalDBNode> dataNodes = conf.getBackupDataNodes();
    Map<String, IPhysicalDBPool> dataHosts = conf.getBackupDataHosts();
    DmdsCluster cluster = conf.getBackupCluster();
    QuarantineConfig quarantine = conf.getBackupQuarantine();

    // 检查可回滚状态
    if (!conf.canRollback()) {
      return false;
    }

    // 如果回滚已经存在的pool
    boolean rollbackStatus = true;
    Map<String, IPhysicalDBPool> cNodes = conf.getDataHosts();
    for (IPhysicalDBPool dn : dataHosts.values()) {
      dn.init(dn.getActivedIndex());
      if (!dn.isInitSuccess()) {
        rollbackStatus = false;
        break;
      }
    }
    // 如果回滚不成功，则清理已初始化的资源。
    if (!rollbackStatus) {
      for (IPhysicalDBPool dn : dataHosts.values()) {
        dn.clearDataSources("rollbackup config");
        dn.stopHeartbeat();
      }
      return false;
    }

    // 应用回滚
    conf.rollback(users, schemas, dataNodes, dataHosts, cluster, quarantine);

    // 处理旧的资源
    for (IPhysicalDBPool dn : cNodes.values()) {
      dn.clearDataSources("clear old config ");
      dn.stopHeartbeat();
    }

    // 清理缓存
    DmdsContext.getInstance().getCacheService().clearCache();
    return true;
  }

}