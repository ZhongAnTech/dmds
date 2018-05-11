/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net;

import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.INIOProcessor;
import com.zhongan.dmds.core.NIOConnection;
import com.zhongan.dmds.core.SocketConnector;
import com.zhongan.dmds.net.util.SelectorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * NIOConnector 处理的是 Connect 事件，是客户端连接服务端事件，就是 Dmds 作为客户端去主动连接 MySQL Server 的操作。
 * 增加1.6 的MIN_SELECT_TIME_IN_NANO_SECONDS
 */
public final class NIOConnector extends Thread implements SocketConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(NIOConnector.class);
  public static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

  private final String name;
  private volatile Selector selector;
  private final BlockingQueue<AbstractConnection> connectQueue;
  private long connectCount;
  private final NIOReactorPool reactorPool;

  public NIOConnector(String name, NIOReactorPool reactorPool) throws IOException {
    super.setName(name);
    this.name = name;
    this.selector = Selector.open();
    this.reactorPool = reactorPool;
    this.connectQueue = new LinkedBlockingQueue<AbstractConnection>();
  }

  public long getConnectCount() {
    return connectCount;
  }

  public void postConnect(NIOConnection c) {
    connectQueue.offer((AbstractConnection) c);
    selector.wakeup();
  }

  @Override
  public void run() {
    int invalidSelectCount = 0;
    for (; ; ) {
      final Selector tSelector = this.selector;
      ++connectCount;
      try {
        long start = System.nanoTime();
        // 查看有无连接就绪
        tSelector.select(1000L);
        long end = System.nanoTime();
        connect(tSelector);
        Set<SelectionKey> keys = tSelector.selectedKeys();
        if (keys.size() == 0 && (end - start) < SelectorUtil.MIN_SELECT_TIME_IN_NANO_SECONDS) {
          invalidSelectCount++;
        } else {
          try {
            for (SelectionKey key : keys) {
              Object att = key.attachment();
              if (att != null && key.isValid() && key.isConnectable()) {
                finishConnect(key, att);
              } else {
                key.cancel();
              }
            }
          } finally {
            invalidSelectCount = 0;
            keys.clear();
          }
        }
        if (invalidSelectCount > SelectorUtil.REBUILD_COUNT_THRESHOLD) {
          final Selector rebuildSelector = SelectorUtil.rebuildSelector(this.selector);
          if (rebuildSelector != null) {
            this.selector = rebuildSelector;
          }
          invalidSelectCount = 0;
        }

      } catch (Exception e) {
        LOGGER.warn(name, e);
      }
    }
  }

  private void connect(Selector selector) {
    AbstractConnection c = null;
    while ((c = connectQueue.poll()) != null) {
      try {
        SocketChannel channel = (SocketChannel) c.getChannel();
        // 注册OP_CONNECT监听与后端连接是否真正建立
        channel.register(selector, SelectionKey.OP_CONNECT, c);
        // 主动连接
        channel.connect(new InetSocketAddress(c.host, c.port));
      } catch (Exception e) {
        c.close(e.toString());
      }
    }
  }

  private void finishConnect(SelectionKey key, Object att) {
    BackendNIOConnection c = (BackendNIOConnection) att;
    try {
      if (finishConnect(c, (SocketChannel) c.channel)) { // 做原生NIO连接是否完成的判断和操作
        clearSelectionKey(key);
        c.setId(ID_GENERATOR.getId());
        // 绑定特定的NIOProcessor以作idle清理
        INIOProcessor processor = DmdsContext.getInstance().nextProcessor();
        c.setProcessor(processor);
        // 与特定NIOReactor绑定监听读写
        NIOReactor reactor = reactorPool.getNextReactor();
        reactor.postRegister(c);
      }
    } catch (Exception e) {
      // 如有异常，将key清空
      clearSelectionKey(key);
      c.close(e.toString());
      c.onConnectFailed(e);
    }
  }

  private boolean finishConnect(AbstractConnection c, SocketChannel channel) throws IOException {
    if (channel.isConnectionPending()) {
      channel.finishConnect();
      c.setLocalPort(channel.socket().getLocalPort());
      return true;
    } else {
      return false;
    }
  }

  private void clearSelectionKey(SelectionKey key) {
    if (key.isValid()) {
      key.attach(null);
      key.cancel();
    }
  }

  /**
   * 后端连接ID生成器
   */
  public static class ConnectIdGenerator {

    private static final long MAX_VALUE = Long.MAX_VALUE;

    private long connectId = 0L;
    private final Object lock = new Object();

    public long getId() {
      synchronized (lock) {
        if (connectId >= MAX_VALUE) {
          connectId = 0L;
        }
        return ++connectId;
      }
    }
  }

}