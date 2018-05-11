/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server.handler;

import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.core.FrontendPrepareHandler;
import com.zhongan.dmds.net.mysql.ByteUtil;
import com.zhongan.dmds.net.mysql.PreparedStatement;
import com.zhongan.dmds.net.protocol.ExecutePacket;
import com.zhongan.dmds.server.ServerConnection;
import com.zhongan.dmds.server.cmd.PreparedStmtResponse;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class ServerPrepareHandler implements FrontendPrepareHandler {

  private ServerConnection source;
  private volatile long pstmtId;
  private Map<String, PreparedStatement> pstmtForSql;
  private Map<Long, PreparedStatement> pstmtForId;

  public ServerPrepareHandler(ServerConnection source) {
    this.source = source;
    this.pstmtId = 0L;
    this.pstmtForSql = new HashMap<String, PreparedStatement>();
    this.pstmtForId = new HashMap<Long, PreparedStatement>();
  }

  @Override
  public void prepare(String sql) {
    PreparedStatement pstmt = null;
    if ((pstmt = pstmtForSql.get(sql)) == null) {
      pstmt = new PreparedStatement(++pstmtId, sql, 0, 0);
      pstmtForSql.put(pstmt.getStatement(), pstmt);
      pstmtForId.put(pstmt.getId(), pstmt);
    }
    PreparedStmtResponse.response(pstmt, source);
  }

  @Override
  public void execute(byte[] data) {
    long pstmtId = ByteUtil.readUB4(data, 5);
    PreparedStatement pstmt = null;
    if ((pstmt = pstmtForSql.get(pstmtId)) == null) {
      source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND,
          "Unknown pstmtId when executing.");
    } else {
      ExecutePacket packet = new ExecutePacket(pstmt);
      try {
        packet.read(data, source.getCharset());
      } catch (UnsupportedEncodingException e) {
        source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, e.getMessage());
        return;
      }
    }
  }

  @Override
  public void close() {

  }

}