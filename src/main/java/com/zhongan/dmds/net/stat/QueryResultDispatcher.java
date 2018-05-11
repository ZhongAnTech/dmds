/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.stat;

import com.zhongan.dmds.commons.stat.QueryConditionAnalyzer;
import com.zhongan.dmds.commons.stat.QueryResult;
import com.zhongan.dmds.commons.stat.QueryResultListener;
import com.zhongan.dmds.commons.stat.TableStatAnalyzer;
import com.zhongan.dmds.core.DmdsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SQL执行后的派发 QueryResult 事件
 */
public class QueryResultDispatcher {

  // 是否派发 QueryResult 事件
  private final static AtomicBoolean isClosed = new AtomicBoolean(false);

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryResultDispatcher.class);

  private static List<QueryResultListener> listeners = new CopyOnWriteArrayList<QueryResultListener>();

  // 初始化强制加载
  static {
    listeners.add(UserStatAnalyzer.getInstance());
    listeners.add(TableStatAnalyzer.getInstance());
    // listeners.add( HighFrequencySqlAnalyzer.getInstance() );
    listeners.add(QueryConditionAnalyzer.getInstance());
  }

  public static boolean close() {
    if (isClosed.compareAndSet(false, true)) {
      return true;
    }
    return false;
  }

  public static boolean open() {
    if (isClosed.compareAndSet(true, false)) {
      return true;
    }
    return false;
  }

  public static void addListener(QueryResultListener listener) {
    if (listener == null) {
      throw new NullPointerException();
    }
    listeners.add(listener);
  }

  public static void removeListener(QueryResultListener listener) {
    listeners.remove(listener);
  }

  public static void removeAllListener() {
    listeners.clear();
  }

  public static void dispatchQuery(final QueryResult queryResult) {
    if (isClosed.get()) {
      return;
    }
    // TODO：异步分发，待进一步调优
    DmdsContext.getInstance().getBusinessExecutor().execute(new Runnable() {
      public void run() {
        for (QueryResultListener listener : listeners) {
          try {
            listener.onQueryResult(queryResult);
          } catch (Throwable e) {
            LOGGER.error("error", e);
          }
        }
      }
    });
  }

}
