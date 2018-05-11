/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.factory;

import java.io.IOException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

/**
 * 2017.01 remove support for AIO, only support NIO
 */
public abstract class BackendConnectionFactory {

  protected NetworkChannel openSocketChannel() throws IOException {
    SocketChannel channel = null;
    channel = SocketChannel.open();
    channel.configureBlocking(false);
    return channel;

  }

}