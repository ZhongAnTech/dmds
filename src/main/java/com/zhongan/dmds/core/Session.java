/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import com.zhongan.dmds.route.RouteResultset;
import com.zhongan.dmds.route.RouteResultsetNode;

import java.util.Map;
import java.util.Set;

/**
 * Extract from NonBlockingSession
 */
public interface Session {

  IServerConnection getSource();

  int getTargetCount();

  Set<RouteResultsetNode> getTargetKeys();

  BackendConnection getTarget(RouteResultsetNode key);

  Map<RouteResultsetNode, BackendConnection> getTargetMap();

  BackendConnection removeTarget(RouteResultsetNode key);

  void execute(RouteResultset rrs, int type);

  void commit();

  void rollback();

  void cancel(NIOConnection sponsor);

  /**
   * {@link IServerConnection#isClosed()} must be true before invoking this
   */
  void terminate();

  void closeAndClearResources(String reason);

  void releaseConnectionIfSafe(BackendConnection conn, boolean debug, boolean needRollback);

  void releaseConnection(RouteResultsetNode rrn, boolean debug, boolean needRollback);

  void releaseConnections(boolean needRollback);

  void releaseConnection(BackendConnection con);

  /**
   * @return previous bound connection
   */
  BackendConnection bindConnection(RouteResultsetNode key, BackendConnection conn);

  boolean tryExistsCon(BackendConnection conn, RouteResultsetNode node);

  void clearResources(boolean needRollback);

  boolean closed();

  void setXATXEnabled(boolean xaTXEnabled);

  String getXaTXID();

}