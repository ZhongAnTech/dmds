/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.backend;

import com.zhongan.dmds.config.Alarms;
import com.zhongan.dmds.config.model.UserConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.FrontendPrivileges;
import com.zhongan.dmds.core.IDmdsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Base on MycatPrivileges
 */
public class DmdsPrivileges implements FrontendPrivileges {

  /**
   * 无需每次建立连接都new实例。
   */
  private static DmdsPrivileges instance = new DmdsPrivileges();

  private static final Logger ALARM = LoggerFactory.getLogger("alarm");

  public static DmdsPrivileges instance() {
    return instance;
  }

  private DmdsPrivileges() {
    super();
  }

  @Override
  public boolean schemaExists(String schema) {
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    return conf.getSchemas().containsKey(schema);
  }

  @Override
  public boolean userExists(String user, String host) {
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    if (conf.getQuarantine().canConnect(host, user, conf.getUsers()) == false) {
      ALARM.error(new StringBuilder().append(Alarms.QUARANTINE_ATTACK).append("[host=").append(host)
          .append(",user=").append(user).append(']').toString());
      return false;
    }
    return true;
  }

  @Override
  public String getPassword(String user) {
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    if (user != null && user.equals(conf.getSystem().getClusterHeartbeatUser())) {
      return conf.getSystem().getClusterHeartbeatPass();
    } else {
      UserConfig uc = conf.getUsers().get(user);
      if (uc != null) {
        return uc.getPassword();
      } else {
        return null;
      }
    }
  }

  @Override
  public Set<String> getUserSchemas(String user) {
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    UserConfig uc = conf.getUsers().get(user);
    if (uc != null) {
      return uc.getSchemas();
    } else {
      return null;
    }
  }

  @Override
  public Boolean isReadOnly(String user) {
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    UserConfig uc = conf.getUsers().get(user);
    if (uc != null) {
      return uc.isReadOnly();
    } else {
      return null;
    }
  }

  @Override
  public int getBenchmark(String user) {
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    UserConfig uc = conf.getUsers().get(user);
    if (uc != null) {
      return uc.getBenchmark();
    } else {
      return 0;
    }
  }

  @Override
  public String getBenchmarkSmsTel(String user) {
    IDmdsConfig conf = DmdsContext.getInstance().getConfig();
    UserConfig uc = conf.getUsers().get(user);
    if (uc != null) {
      return uc.getBenchmarkSmsTel();
    } else {
      return null;
    }
  }

}