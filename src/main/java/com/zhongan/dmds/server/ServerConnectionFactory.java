/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.server;

import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.net.backend.DmdsPrivileges;
import com.zhongan.dmds.net.factory.FrontendConnectionFactory;
import com.zhongan.dmds.net.frontend.FrontendConnection;
import com.zhongan.dmds.net.session.NonBlockingSession;
import com.zhongan.dmds.server.handler.ServerLoadDataInfileHandler;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

public class ServerConnectionFactory extends FrontendConnectionFactory {

  @Override
  protected FrontendConnection getConnection(NetworkChannel channel) throws IOException {
    SystemConfig sys = DmdsContext.getInstance().getSystem();
    ServerConnection c = new ServerConnection(channel);
    DmdsContext.getInstance().getConfig().setSocketParams(c, true);
    c.setPrivileges(DmdsPrivileges.instance());
    c.setQueryHandler(new ServerQueryHandler(c));
    c.setLoadDataInfileHandler(new ServerLoadDataInfileHandler(c));
    // c.setPrepareHandler(new ServerPrepareHandler(c));
    c.setTxIsolation(sys.getTxIsolation());
    // 创建绑定唯一Session
    c.setSession2(new NonBlockingSession(c));
    return c;
  }

}