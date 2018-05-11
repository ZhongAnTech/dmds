/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio.handler;

import com.zhongan.dmds.commons.util.StringUtil;
import com.zhongan.dmds.config.ErrorCode;
import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.ResponseHandler;
import com.zhongan.dmds.core.Session;
import com.zhongan.dmds.net.protocol.ErrorPacket;
import com.zhongan.dmds.net.session.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public abstract class MultiNodeHandler implements ResponseHandler, Terminatable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeHandler.class);
  protected final ReentrantLock lock = new ReentrantLock();
  protected final Session session;
  private AtomicBoolean isFailed = new AtomicBoolean(false);
  protected volatile String error;
  protected byte packetId;
  protected final AtomicBoolean errorRepsponsed = new AtomicBoolean(false);

  public MultiNodeHandler(NonBlockingSession session) {
    if (session == null) {
      throw new IllegalArgumentException("session is null!");
    }
    this.session = session;
  }

  public void setFail(String errMsg) {
    isFailed.set(true);
    error = errMsg;
  }

  public boolean isFail() {
    return isFailed.get();
  }

  private int nodeCount;

  private Runnable terminateCallBack;

  @Override
  public void terminate(Runnable terminateCallBack) {
    boolean zeroReached = false;
    lock.lock();
    try {
      if (nodeCount > 0) {
        this.terminateCallBack = terminateCallBack;
      } else {
        zeroReached = true;
      }
    } finally {
      lock.unlock();
    }
    if (zeroReached) {
      terminateCallBack.run();
    }
  }

  protected boolean canClose(BackendConnection conn, boolean tryErrorFinish) {

    // realse this connection if safe
    session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
    boolean allFinished = false;
    if (tryErrorFinish) {
      allFinished = this.decrementCountBy(1);
      this.tryErrorFinished(allFinished);
    }

    return allFinished;
  }

  protected void decrementCountToZero() {
    Runnable callback;
    lock.lock();
    try {
      nodeCount = 0;
      callback = this.terminateCallBack;
      this.terminateCallBack = null;
    } finally {
      lock.unlock();
    }
    if (callback != null) {
      callback.run();
    }
  }

  public void connectionError(Throwable e, BackendConnection conn) {
    final boolean canClose = decrementCountBy(1);
    // 需要把Throwable e的错误信息保存下来（setFail()）， 否则会导致响应
    // null信息，结果mysql命令行等客户端查询结果是"Query OK"！！
    // @author Uncle-pan
    // @since 2016-03-26
    if (canClose) {
      setFail("backend connect: " + e);
    }
    LOGGER.warn("backend connect", e);
    this.tryErrorFinished(canClose);
  }

  public void errorResponse(byte[] data, BackendConnection conn) {
    session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
    ErrorPacket err = new ErrorPacket();
    err.read(data);
    String errmsg = new String(err.message);
    this.setFail(errmsg);
    LOGGER.warn("error response from " + conn + " err " + errmsg + " code:" + err.errno);

    this.tryErrorFinished(this.decrementCountBy(1));
  }

  public boolean clearIfSessionClosed(Session session) {
    if (session.closed()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("session closed ,clear resources " + session);
      }

      session.clearResources(true);
      this.clearResources();
      return true;
    } else {
      return false;
    }

  }

  protected boolean decrementCountBy(int finished) {
    boolean zeroReached = false;
    Runnable callback = null;
    lock.lock();
    try {
      if (zeroReached = --nodeCount == 0) {
        callback = this.terminateCallBack;
        this.terminateCallBack = null;
      }
    } finally {
      lock.unlock();
    }
    if (zeroReached && callback != null) {
      callback.run();
    }
    return zeroReached;
  }

  protected void reset(int initCount) {
    nodeCount = initCount;
    isFailed.set(false);
    error = null;
    packetId = 0;
  }

  protected ErrorPacket createErrPkg(String errmgs) {
    ErrorPacket err = new ErrorPacket();
    lock.lock();
    try {
      err.packetId = ++packetId;
    } finally {
      lock.unlock();
    }
    err.errno = ErrorCode.ER_UNKNOWN_ERROR;
    err.message = StringUtil.encode(errmgs, session.getSource().getCharset());
    return err;
  }

  protected void tryErrorFinished(boolean allEnd) {
    if (allEnd && !session.closed()) {
      if (errorRepsponsed.compareAndSet(false, true)) {
        createErrPkg(this.error).write(session.getSource());
      }
      // clear session resources,release all
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("error all end ,clear session resource ");
      }
      if (session.getSource().isAutocommit()) {
        session.closeAndClearResources(error);
      } else {
        session.getSource().setTxInterrupt(this.error);
        // clear resouces
        clearResources();
      }

    }

  }

  public void connectionClose(BackendConnection conn, String reason) {
    this.setFail("closed connection:" + reason + " con:" + conn);
    boolean finished = false;
    lock.lock();
    try {
      finished = (this.nodeCount == 0);

    } finally {
      lock.unlock();
    }
    if (finished == false) {
      finished = this.decrementCountBy(1);
    }
    if (error == null) {
      error = "back connection closed ";
    }
    tryErrorFinished(finished);
  }

  public void clearResources() {
  }
}