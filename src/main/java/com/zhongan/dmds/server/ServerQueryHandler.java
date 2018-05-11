/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server;

import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.core.FrontendQueryHandler;
import com.zhongan.dmds.net.protocol.OkPacket;
import com.zhongan.dmds.server.handler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerQueryHandler implements FrontendQueryHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryHandler.class);

  private final ServerConnection source;
  protected Boolean readOnly;

  public void setReadOnly(Boolean readOnly) {
    this.readOnly = readOnly;
  }

  public ServerQueryHandler(ServerConnection source) {
    this.source = source;
  }

  @Override
  public void query(String sql) {

    ServerConnection c = this.source;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(new StringBuilder().append(c).append(sql).toString());
    }
    int rs = ServerParse.parse(sql);
    int sqlType = rs & 0xff;

    switch (sqlType) {
      case ServerParse.EXPLAIN:
        ExplainHandler.handle(sql, c, rs >>> 8);
        break;
      case ServerParse.EXPLAIN2:
        Explain2Handler.handle(sql, c, rs >>> 8);
        break;
      case ServerParse.SET:
        SetHandler.handle(sql, c, rs >>> 8);
        break;
      case ServerParse.SHOW:
        ShowHandler.handle(sql, c, rs >>> 8);
        break;
      case ServerParse.SELECT:
        if (QuarantineHandler.handle(sql, c)) {
          SelectHandler.handle(sql, c, rs >>> 8);
        }
        break;
      case ServerParse.START:
        StartHandler.handle(sql, c, rs >>> 8);
        break;
      case ServerParse.BEGIN:
        BeginHandler.handle(sql, c);
        break;
      case ServerParse.SAVEPOINT:
        SavepointHandler.handle(sql, c);
        break;
      case ServerParse.KILL:
        KillHandler.handle(sql, rs >>> 8, c);
        break;
      case ServerParse.KILL_QUERY:
        LOGGER.warn(new StringBuilder().append("Unsupported command:").append(sql).toString());
        c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported command");
        break;
      case ServerParse.USE:
        UseHandler.handle(sql, c, rs >>> 8);
        break;
      case ServerParse.COMMIT:
        c.commit();
        break;
      case ServerParse.ROLLBACK:
        c.rollback();
        break;
      case ServerParse.HELP:
        LOGGER.warn(new StringBuilder().append("Unsupported command:").append(sql).toString());
        c.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "Unsupported command");
        break;
      case ServerParse.MYSQL_CMD_COMMENT:
        c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        break;
      case ServerParse.MYSQL_COMMENT:
        c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        break;
      case ServerParse.LOAD_DATA_INFILE_SQL:
        c.loadDataInfileStart(sql);
        break;
      default:
        if (readOnly) {
          LOGGER.warn(new StringBuilder().append("User readonly:").append(sql).toString());
          c.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User readonly");
          break;
        }
        if (QuarantineHandler.handle(sql, c)) {
          c.execute(sql, rs & 0xff);
        }
    }
  }

}