/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net;

import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.INIOProcessor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.NetworkChannel;


/**
 * Base on BackendAIOConnection
 */
public abstract class BackendNIOConnection extends AbstractConnection implements BackendConnection {

  protected boolean isFinishConnect;

  public BackendNIOConnection(NetworkChannel channel) {
    super(channel);
  }

  public void register() throws IOException {
    this.asynRead();
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void discardClose(String reason) {
    // 跨节点处理,中断后端连接时关闭
  }

  public abstract void onConnectFailed(Throwable e);

  public boolean finishConnect() throws IOException {
    localPort = ((InetSocketAddress) channel.getLocalAddress()).getPort();
    isFinishConnect = true;
    return true;
  }

  public void setProcessor(INIOProcessor processor) {
    super.setProcessor(processor);
    processor.addBackend(this);
  }

  @Override
  public String toString() {
    return "BackendConnection [id=" + id + ", host=" + host + ", port=" + port + ", localPort="
        + localPort + "]";
  }

}