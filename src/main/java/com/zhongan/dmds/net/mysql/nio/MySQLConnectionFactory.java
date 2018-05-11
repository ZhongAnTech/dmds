/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio;

import com.zhongan.dmds.config.model.DBHostConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.ResponseHandler;
import com.zhongan.dmds.net.NIOConnector;
import com.zhongan.dmds.net.factory.BackendConnectionFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;

public class MySQLConnectionFactory extends BackendConnectionFactory {

  @SuppressWarnings({"unchecked", "rawtypes"})
  public MySQLConnection make(MySQLDataSource pool, ResponseHandler handler, String schema)
      throws IOException {
    // DBHost配置
    DBHostConfig dsc = pool.getConfig();
    // 根据是否为NIO返回SocketChannel或者AIO的AsynchronousSocketChannel
    NetworkChannel channel = openSocketChannel();
    // 新建MySQLConnection
    MySQLConnection c = new MySQLConnection(channel, pool.isReadNode());
    // 根据配置初始化MySQLConnection
    DmdsContext.getInstance().getConfig().setSocketParams(c, false);
    c.setHost(dsc.getIp());
    c.setPort(dsc.getPort());
    c.setUser(dsc.getUser());
    c.setPassword(dsc.getPassword());
    c.setSchema(schema);
    // 目前实际连接还未建立，handler为MySQL连接认证MySQLConnectionAuthenticator，传入的handler为后端连接处理器ResponseHandler
    c.setHandler(new MySQLConnectionAuthenticator(c, handler));
    c.setPool(pool);
    c.setIdleTimeout(pool.getConfig().getIdleTimeout());
    // AIO和NIO连接方式建立实际的MySQL连接
    if (channel instanceof AsynchronousSocketChannel) {
      ((AsynchronousSocketChannel) channel)
          .connect(new InetSocketAddress(dsc.getIp(), dsc.getPort()), c,
              (CompletionHandler) DmdsContext.getInstance().getConnector());
    } else {
      // 通过NIOConnector建立连接
      ((NIOConnector) DmdsContext.getInstance().getConnector()).postConnect(c);

    }
    return c;
  }

}