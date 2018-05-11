/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net;

import com.zhongan.dmds.net.util.SelectorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 网络事件反应器
 * <p>
 * Catch exceptions such as OOM so that the reactor can keep running for response client!
 * 增加1.6 的MIN_SELECT_TIME_IN_NANO_SECONDS和REBUILD_COUNT_THRESHOLD
 */
public final class NIOReactor {

  private static final Logger LOGGER = LoggerFactory.getLogger(NIOReactor.class);
  private final String name;
  private final RW reactorRW;

  public NIOReactor(String name) throws IOException {
    this.name = name;
    this.reactorRW = new RW();
  }

  final void startup() {
    new Thread(reactorRW, name + "-RW").start();
  }

  final void postRegister(AbstractConnection c) {
    reactorRW.registerQueue.offer(c);
    reactorRW.selector.wakeup();
  }

  final Queue<AbstractConnection> getRegisterQueue() {
    return reactorRW.registerQueue;
  }

  final long getReactCount() {
    return reactorRW.reactCount;
  }

  private final class RW implements Runnable {

    //选择器需要实时同步到主存
    private volatile Selector selector;
    private final ConcurrentLinkedQueue<AbstractConnection> registerQueue;
    private long reactCount;

    private RW() throws IOException {
      this.selector = Selector.open();
      this.registerQueue = new ConcurrentLinkedQueue<AbstractConnection>();
    }

    @Override
    public void run() {
      int invalidSelectCount = 0;
      Set<SelectionKey> keys = null;
      for (; ; ) {
        ++reactCount;
        try {
          //每次获取新的selector
          final Selector selector = this.selector;

          long start = System.nanoTime();
          selector.select(500L);
          long end = System.nanoTime();

          register(selector);

          keys = selector.selectedKeys();
          if (keys.size() == 0 && (end - start) < SelectorUtil.MIN_SELECT_TIME_IN_NANO_SECONDS) {
            invalidSelectCount++;
          } else {
            invalidSelectCount = 0;
            for (SelectionKey key : keys) {
              AbstractConnection con = null;
              try {
                Object att = key.attachment();
                if (att != null) {
                  con = (AbstractConnection) att;
                  if (key.isValid() && key.isReadable()) {
                    try {
                      con.asynRead();
                    } catch (IOException e) {
                      LOGGER.warn("网络IO异常参数： con.channel.isOpen:" + con.channel.isOpen()
                          + ", con.isClosed: " + con.isClosed + ", processor.Name: " + con
                          .getProcessor().getName() + ", con: " + con);
                      con.close("program err:" + e.toString());
                      continue;
                    } catch (Exception e) {
                      LOGGER.warn("caught err: ", e);
                      con.close("program err:" + e.toString());
                      continue;
                    }
                  }
                  if (key.isValid() && key.isWritable()) {
                    con.doNextWriteCheck();
                  }
                } else {
                  key.cancel();
                }
              } catch (CancelledKeyException e) {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(con + " socket key canceled");
                }
              } catch (Exception e) {
                LOGGER.warn(con + " " + e);
              } catch (final Throwable e) {
                // Catch exceptions such as OOM and close connection if exists
                // so that the reactor can keep running!
                // @author Uncle-pan
                // @since 2016-03-30
                if (con != null) {
                  con.close("Bad: " + e);
                }
                LOGGER.error("caught err: ", e);
                continue;
              }
            }//for
          }//else

          if (invalidSelectCount > SelectorUtil.REBUILD_COUNT_THRESHOLD) {
            final Selector rebuildSelector = SelectorUtil.rebuildSelector(this.selector);
            if (rebuildSelector != null) {
              this.selector = rebuildSelector;
            }
            invalidSelectCount = 0;
          }
        } catch (Exception e) {
          LOGGER.warn(name, e);
        } catch (final Throwable e) {
          // Catch exceptions such as OOM so that the reactor can keep running!
          LOGGER.error("caught err: ", e);
        } finally {
          if (keys != null) {
            keys.clear();
          }
        }
      }
    }

    private void register(Selector selector) {
      AbstractConnection c = null;
      if (registerQueue.isEmpty()) {
        return;
      }
      while ((c = registerQueue.poll()) != null) {
        try {
          ((NIOSocketWR) c.getSocketWR()).register(selector);
          c.register();
        } catch (Exception e) {
          c.close("register err" + e.toString());
        }
      }
    }
  }
}