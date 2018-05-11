/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.sqlcmd;

import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.Session;
import com.zhongan.dmds.net.protocol.ErrorPacket;

public class CommitCommand implements SQLCtrlCommand {

  @Override
  public void sendCommand(Session session, BackendConnection con) {
    con.commit();
  }

  @Override
  public void errorResponse(Session session, byte[] err, int total, int failed) {
    ErrorPacket errPkg = new ErrorPacket();
    errPkg.read(err);
    String errInfo =
        "total " + total + " failed " + failed + " detail:" + new String(errPkg.message);
    session.getSource().setTxInterrupt(errInfo);
    errPkg.write(session.getSource());
  }

  @Override
  public void okResponse(Session session, byte[] ok) {
    session.getSource().write(ok);
  }

  @Override
  public boolean releaseConOnErr() {
    // need rollback when err
    return false;
  }

  @Override
  public boolean relaseConOnOK() {
    return true;
  }

  @Override
  public boolean isAutoClearSessionCons() {
    // need rollback when err
    return false;
  }

}
