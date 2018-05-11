/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.handler;

import com.zhongan.dmds.commons.util.SecurityUtil;
import com.zhongan.dmds.config.Capabilities;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.FrontendPrivileges;
import com.zhongan.dmds.core.INIOProcessor;
import com.zhongan.dmds.core.NIOHandler;
import com.zhongan.dmds.net.frontend.FrontendConnection;
import com.zhongan.dmds.net.protocol.AuthPacket;
import com.zhongan.dmds.net.protocol.MySQLPacket;
import com.zhongan.dmds.net.protocol.QuitPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

/**
 * 前端认证处理器
 */
public class FrontendAuthenticator implements NIOHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(FrontendAuthenticator.class);
  private static final byte[] AUTH_OK = new byte[]{7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0};

  protected final FrontendConnection source;

  public FrontendAuthenticator(FrontendConnection source) {
    this.source = source;
  }

  @Override
  public void handle(byte[] data) {
    // check quit packet
    if (data.length == QuitPacket.QUIT.length && data[4] == MySQLPacket.COM_QUIT) {
      source.close("quit packet");
      return;
    }

    AuthPacket auth = new AuthPacket();
    auth.read(data);

    // check user
    if (!checkUser(auth.user, source.getHost())) {
      failure(ErrorCode.ER_ACCESS_DENIED_ERROR,
          "Access denied for user '" + auth.user + "' with host '" + source.getHost() + "'");
      return;
    }

    // check password
    if (!checkPassword(auth.password, auth.user)) {
      failure(ErrorCode.ER_ACCESS_DENIED_ERROR,
          "Access denied for user '" + auth.user + "', because password is error ");
      return;
    }

    // check degrade
    if (isDegrade(auth.user)) {
      failure(ErrorCode.ER_ACCESS_DENIED_ERROR,
          "Access denied for user '" + auth.user + "', because service be degraded ");
      return;
    }

    // check schema
    switch (checkSchema(auth.database, auth.user)) {
      case ErrorCode.ER_BAD_DB_ERROR:
        failure(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + auth.database + "'");
        break;
      case ErrorCode.ER_DBACCESS_DENIED_ERROR:
        String s = "Access denied for user '" + auth.user + "' to database '" + auth.database + "'";
        failure(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
        break;
      default:
        success(auth);
    }
  }

  // TODO: add by zhuam
  // 前端 connection 达到该用户设定的阀值后, 立马降级拒绝连接
  protected boolean isDegrade(String user) {

    int benchmark = source.getPrivileges().getBenchmark(user);
    if (benchmark > 0) {

      int forntedsLength = 0;
      INIOProcessor[] processors = DmdsContext.getInstance().getProcessors();
      for (INIOProcessor p : processors) {
        forntedsLength += p.getForntedsLength();
      }

      if (forntedsLength >= benchmark) {
        return true;
      }
    }

    return false;
  }

  protected boolean checkUser(String user, String host) {
    return source.getPrivileges().userExists(user, host);
  }

  protected boolean checkPassword(byte[] password, String user) {
    String pass = source.getPrivileges().getPassword(user);

    // check null
    if (pass == null || pass.length() == 0) {
      if (password == null || password.length == 0) {
        return true;
      } else {
        return false;
      }
    }
    if (password == null || password.length == 0) {
      return false;
    }

    // encrypt
    byte[] encryptPass = null;
    try {
      encryptPass = SecurityUtil.scramble411(pass.getBytes(), source.getSeed());
    } catch (NoSuchAlgorithmException e) {
      LOGGER.warn(source.toString(), e);
      return false;
    }
    if (encryptPass != null && (encryptPass.length == password.length)) {
      int i = encryptPass.length;
      while (i-- != 0) {
        if (encryptPass[i] != password[i]) {
          return false;
        }
      }
    } else {
      return false;
    }

    return true;
  }

  protected int checkSchema(String schema, String user) {
    if (schema == null) {
      return 0;
    }
    FrontendPrivileges privileges = source.getPrivileges();
    if (!privileges.schemaExists(schema)) {
      return ErrorCode.ER_BAD_DB_ERROR;
    }
    Set<String> schemas = privileges.getUserSchemas(user);
    if (schemas == null || schemas.size() == 0 || schemas.contains(schema)) {
      return 0;
    } else {
      return ErrorCode.ER_DBACCESS_DENIED_ERROR;
    }
  }

  protected void success(AuthPacket auth) {
    source.setAuthenticated(true);
    source.setUser(auth.user);
    source.setSchema(auth.database);
    source.setCharsetIndex(auth.charsetIndex);
    source.setHandler(new FrontendCommandHandler(source));

    if (LOGGER.isInfoEnabled()) {
      StringBuilder s = new StringBuilder();
      s.append(source).append('\'').append(auth.user).append("' login success");
      byte[] extra = auth.extra;
      if (extra != null && extra.length > 0) {
        s.append(",extra:").append(new String(extra));
      }
      LOGGER.info(s.toString());
    }

    ByteBuffer buffer = source.allocate();
    source.write(source.writeToBuffer(AUTH_OK, buffer));
    boolean clientCompress =
        Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & auth.clientFlags);
    boolean usingCompress = DmdsContext.getInstance().getSystem().getUseCompression() == 1;
    if (clientCompress && usingCompress) {
      source.setSupportCompress(true);
    }
  }

  protected void failure(int errno, String info) {
    LOGGER.error(source.toString() + info);
    source.writeErrMessage((byte) 2, errno, info);
  }

}