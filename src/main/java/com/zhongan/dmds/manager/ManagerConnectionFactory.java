/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager;

import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.net.backend.DmdsPrivileges;
import com.zhongan.dmds.net.factory.FrontendConnectionFactory;
import com.zhongan.dmds.net.frontend.FrontendConnection;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

public class ManagerConnectionFactory extends FrontendConnectionFactory {

  @Override
  protected FrontendConnection getConnection(NetworkChannel channel) throws IOException {
    ManagerConnection c = new ManagerConnection(channel);
    DmdsContext.getInstance().getConfig().setSocketParams(c, true);
    c.setPrivileges(DmdsPrivileges.instance());
    c.setQueryHandler(new ManagerQueryHandler(c));
    return c;
  }

}