/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.handler;

import com.zhongan.dmds.commons.mysql.MySQLMessage;
import com.zhongan.dmds.commons.statistic.CommandCount;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.core.NIOHandler;
import com.zhongan.dmds.net.frontend.FrontendConnection;
import com.zhongan.dmds.net.protocol.MySQLPacket;

/**
 * 前端命令处理器
 */
public class FrontendCommandHandler implements NIOHandler {

  protected final FrontendConnection source;
  protected final CommandCount commands;

  public FrontendCommandHandler(FrontendConnection source) {
    this.source = source;
    this.commands = source.getProcessor().getCommands();
  }

  @Override
  public void handle(byte[] data) {
    if (source.getLoadDataInfileHandler() != null && source.getLoadDataInfileHandler()
        .isStartLoadData()) {
      MySQLMessage mm = new MySQLMessage(data);
      int packetLength = mm.readUB3();
      if ((packetLength + 4) == data.length) {
        source.loadDataInfileData(data);
      }
      return;
    }
    switch (data[4]) {
      case MySQLPacket.COM_INIT_DB:
        commands.doInitDB();
        source.initDB(data);
        break;
      case MySQLPacket.COM_QUERY:
        commands.doQuery();
        source.query(data);
        break;
      case MySQLPacket.COM_PING:
        commands.doPing();
        source.ping();
        break;
      case MySQLPacket.COM_QUIT:
        commands.doQuit();
        source.close("handle quit cmd");
        break;
      case MySQLPacket.COM_PROCESS_KILL:
        commands.doKill();
        source.kill(data);
        break;
      case MySQLPacket.COM_STMT_PREPARE:
        commands.doStmtPrepare();
        source.stmtPrepare(data);
        break;
      case MySQLPacket.COM_STMT_EXECUTE:
        commands.doStmtExecute();
        source.stmtExecute(data);
        break;
      case MySQLPacket.COM_STMT_CLOSE:
        commands.doStmtClose();
        source.stmtClose(data);
        break;
      case MySQLPacket.COM_HEARTBEAT:
        commands.doHeartbeat();
        source.heartbeat(data);
        break;
      default:
        commands.doOther();
        source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");

    }
  }

}