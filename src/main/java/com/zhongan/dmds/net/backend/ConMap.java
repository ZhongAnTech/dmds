/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.backend;

import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.INIOProcessor;
import com.zhongan.dmds.net.jdbc.JDBCConnection;
import com.zhongan.dmds.net.mysql.nio.MySQLConnection;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConMap {

  // key -schema
  private final ConcurrentHashMap<String, ConQueue> items = new ConcurrentHashMap<String, ConQueue>();

  // key -datahost gwd 存储每个datahost创建的连接数
  private static final ConcurrentHashMap<String, AtomicInteger> dataHostConnNumMap = new ConcurrentHashMap<String, AtomicInteger>();

  // 添加连接数 gwd
  public static void addConnectionNum(String dataHost) {
    AtomicInteger num = dataHostConnNumMap.get(dataHost);
    if (num == null) {
      dataHostConnNumMap.put(dataHost, new AtomicInteger(1));
    } else {
      num.incrementAndGet();
      dataHostConnNumMap.put(dataHost, num);
    }
  }

  // 删除连接数 gwd
  public static void removeConnectionNum(String dataHost) {
    AtomicInteger num = dataHostConnNumMap.get(dataHost);
    if (num != null) {
      num.decrementAndGet();
      if (num.intValue() == -1) {
        num.set(0);
      }
      dataHostConnNumMap.put(dataHost, num);
    }
  }

  // 得到连接数 gwd
  public static int getConnectionNumByDataHost(String dataHost) {
    AtomicInteger num = dataHostConnNumMap.get(dataHost);
    return num == null ? 0 : num.get();
  }

  public ConQueue getSchemaConQueue(String schema) {
    ConQueue queue = items.get(schema);
    if (queue == null) {
      ConQueue newQueue = new ConQueue();
      queue = items.putIfAbsent(schema, newQueue);
      return (queue == null) ? newQueue : queue;
    }
    return queue;
  }

  public BackendConnection tryTakeCon(final String schema, boolean autoCommit) {
    final ConQueue queue = items.get(schema);
    BackendConnection con = tryTakeCon(queue, autoCommit);
    if (con != null) {
      return con;
    } else {
      for (ConQueue queue2 : items.values()) {
        if (queue != queue2) {
          con = tryTakeCon(queue2, autoCommit);
          if (con != null) {
            return con;
          }
        }
      }
    }
    return null;

  }

  private BackendConnection tryTakeCon(ConQueue queue, boolean autoCommit) {

    BackendConnection con = null;
    if (queue != null && ((con = queue.takeIdleCon(autoCommit)) != null)) {
      return con;
    } else {
      return null;
    }

  }

  public Collection<ConQueue> getAllConQueue() {
    return items.values();
  }

  public int getActiveCountForSchema(String schema, PhysicalDatasource dataSouce) {
    int total = 0;
    for (INIOProcessor processor : DmdsContext.getInstance().getProcessors()) {
      for (BackendConnection con : processor.getBackends().values()) {
        if (con instanceof MySQLConnection) {
          MySQLConnection mysqlCon = (MySQLConnection) con;

          if (mysqlCon.getSchema().equals(schema) && mysqlCon.getPool() == dataSouce) {
            if (mysqlCon.isBorrowed()) {
              total++;
            }
          }

        } else if (con instanceof JDBCConnection) {
          JDBCConnection jdbcCon = (JDBCConnection) con;
          if (jdbcCon.getSchema().equals(schema) && jdbcCon.getPool() == dataSouce) {
            if (jdbcCon.isBorrowed()) {
              total++;
            }
          }
        }
      }
    }
    return total;
  }

  public int getActiveCountForDs(PhysicalDatasource dataSouce) {
    int total = 0;
    for (INIOProcessor processor : DmdsContext.getInstance().getProcessors()) {
      for (BackendConnection con : processor.getBackends().values()) {
        if (con instanceof MySQLConnection) {
          MySQLConnection mysqlCon = (MySQLConnection) con;

          if (mysqlCon.getPool() == dataSouce) {
            if (mysqlCon.isBorrowed() && !mysqlCon.isClosed()) {
              total++;
            }
          }

        } else if (con instanceof JDBCConnection) {
          JDBCConnection jdbcCon = (JDBCConnection) con;
          if (jdbcCon.getPool() == dataSouce) {
            if (jdbcCon.isBorrowed() && !jdbcCon.isClosed()) {
              total++;
            }
          }
        }
      }
    }
    return total;
  }

  public void clearConnections(String reason, PhysicalDatasource dataSouce) {
    for (INIOProcessor processor : DmdsContext.getInstance().getProcessors()) {
      ConcurrentMap<Long, BackendConnection> map = processor.getBackends();
      Iterator<Entry<Long, BackendConnection>> itor = map.entrySet().iterator();
      while (itor.hasNext()) {
        Entry<Long, BackendConnection> entry = itor.next();
        BackendConnection con = entry.getValue();
        if (con instanceof MySQLConnection) {
          if (((MySQLConnection) con).getPool() == dataSouce) {
            con.close(reason);
            itor.remove();
          }
        } else if (con instanceof JDBCConnection) {
          if (((JDBCConnection) con).getPool() == dataSouce) {
            con.close(reason);
            itor.remove();
          }
        }
      }

    }
    items.clear();
  }

}
