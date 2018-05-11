/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.handler;

import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.config.model.QuarantineConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class QuarantineHandler {

  private static Logger logger = LoggerFactory.getLogger(QuarantineHandler.class);
  private static boolean check = false;

  private final static ThreadLocal<WallProvider> contextLocal = new ThreadLocal<WallProvider>();

  public static boolean handle(String sql, IServerConnection c) {
    if (contextLocal.get() == null) {
      QuarantineConfig quarantineConfig = DmdsContext.getInstance().getConfig().getQuarantine();
      if (quarantineConfig != null) {
        if (quarantineConfig.isCheck()) {
          contextLocal.set(quarantineConfig.getProvider());
          check = true;
        }
      }
    }
    if (check) {
      WallCheckResult result = contextLocal.get().check(sql);
      if (!result.getViolations().isEmpty()) {
        logger.warn(result.getViolations().get(0).getMessage());
        c.writeErrMessage(ErrorCode.ERR_WRONG_USED, result.getViolations().get(0).getMessage());
        return false;
      }
    }

    return true;
  }

}