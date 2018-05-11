/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model;

import com.zhongan.dmds.config.Isolations;

import java.io.File;
import java.io.IOException;

/**
 * 系统基础配置项
 *
 * remove support for sequence handler
 */
public final class SystemConfig {

  public static final String SYS_HOME = "DMDS_HOME";
  private static final int DEFAULT_PORT = 8066;
  private static final int DEFAULT_MANAGER_PORT = 9066;
  private static final String DEFAULT_CHARSET = "utf8";

  private static final String DEFAULT_SQL_PARSER = "druidparser";// fdbparser, druidparser
  private static final int DEFAULT_PROCESSORS = Runtime.getRuntime().availableProcessors();
  private static final int DEFAULT_BUFFER_CHUNK_SIZE = 4096;
  private static final long DEFAULT_PROCESSOR_CHECK_PERIOD = 1 * 1000L;
  /*
   *每5分钟查询一下处于idle的backend端的线程，如果大于初始化的线程数数 则close掉 ，如果没有小于初始化线程数则创建
   */
  private static final long DEFAULT_DATANODE_IDLE_CHECK_PERIOD = 5 * 60 * 1000L;
  private static final long DEFAULT_DATANODE_HEARTBEAT_PERIOD = 10 * 1000L;
  private static final long DEFAULT_CLUSTER_HEARTBEAT_PERIOD = 5 * 1000L;
  private static final long DEFAULT_CLUSTER_HEARTBEAT_TIMEOUT = 10 * 1000L;
  private static final int DEFAULT_CLUSTER_HEARTBEAT_RETRY = 10;
  private static final int DEFAULT_MAX_LIMIT = 100;
  private static final String DEFAULT_CLUSTER_HEARTBEAT_USER = "_HEARTBEAT_USER_";
  private static final String DEFAULT_CLUSTER_HEARTBEAT_PASS = "_HEARTBEAT_PASS_";
  private static final int DEFAULT_PARSER_COMMENT_VERSION = 50148;
  private static final int DEFAULT_SQL_RECORD_COUNT = 10;
  private int maxStringLiteralLength = 65535;
  private int frontWriteQueueSize = 2048;
  private String bindIp = "0.0.0.0";

  /*
   * 每个processor占用的buffer比例，默认100%
   */
  private int processorBufferLocalPercent;
  private int frontSocketSoRcvbuf = 1024 * 1024;
  private int frontSocketSoSndbuf = 4 * 1024 * 1024;
  private int backSocketSoRcvbuf = 4 * 1024 * 1024;// mysql 5.6
  // net_buffer_length
  // defaut 4M
  private int backSocketSoSndbuf = 1024 * 1024;
  private int frontSocketNoDelay = 1; // 0=false
  private int backSocketNoDelay = 1; // 1=true

  public static final int DEFAULT_POOL_SIZE = 128;// 保持后端数据通道的默认最大值
  public static final long DEFAULT_IDLE_TIMEOUT =
      10 * 60 * 1000L; //某连接在发起空闲检查下，发现距离上次使用超过了空闲时间，那么这个连接会被回收，就是被直接的关闭掉。默认 30 分钟，单位毫秒
  // DBA推荐10分钟
  private String fakeMySQLVersion = null;
  private int serverPort;
  private int managerPort;
  private String charset;
  /*
   * 这个属性主要用于指定系统可用的线程数，默认值为机器 CPU 核心线程数。 主要影响 processorBufferPool、
   * processorBufferLocalPercent、 processorExecutor 属性。 NIOProcessor
   * 的个数也是由这个属性定义的，所以调优的时候可以适当的调高这个属性
   */
  private int processors;

  /*
   * 指定 NIOProcessor 上共享的 businessExecutor 固定线程池大小
   * 对应的是执行实例：BusinessExecutor，线程数量：threadPoolSize
   */
  private int processorExecutor;
  private int timerExecutor;
  private int managerExecutor;
  private long idleTimeout;

  /*
   * 后端sql执行超时时间(秒)
   */
  private long sqlExecuteTimeout = 300;

  /*
   * 清理 NIOProcessor 上前后端空闲、超时和关闭连接的间隔时间，默认1秒 (ms)
   */
  private long processorCheckPeriod;

  /*
   * 对后端连接进行空闲、超时检查的时间间隔，默认是 300 秒(ms)
   */
  private long dataNodeIdleCheckPeriod;

  /*
   * 对后端所有读、写库发起心跳的间隔时间，默认是 10 秒，单位毫秒
   */
  private long dataNodeHeartbeatPeriod;
  private String clusterHeartbeatUser;
  private String clusterHeartbeatPass;
  private long clusterHeartbeatPeriod;
  private long clusterHeartbeatTimeout;
  private int clusterHeartbeatRetry;
  private int txIsolation;
  private int parserCommentVersion;
  private int sqlRecordCount;

  /*
   * processor总大小(byte) 这个属性指定 bufferPool 计算 比例值。由于每次执行 NIO 读、写操作都需要使用到 buffer，
   * 系统初始化的时候会建立一定长度的 buffer 池来加快读、写的效率，减少建立 buffer 的时间
   */
  private long processorBufferPool;
  /*
   * 每个processor大小(byte) 默认 4096 byte
   */
  private int processorBufferChunk;
  private int defaultMaxLimit = DEFAULT_MAX_LIMIT;
  /*
   * 注意！！！ 目前dmds支持的MySQL版本，如果后续有新的MySQL版本,请添加到此数组， 对于MySQL的其他分支，
   * 比如MariaDB目前版本号已经到10.1.x，但是其驱动程序仍然兼容官方的MySQL,因此这里版本号只需要MySQL官方的版本号即可。
   */
  public static final String[] MySQLVersions = {"5.5", "5.6", "5.7"};

  private String sqlInterceptor = "com.zhongan.dmds.manager.interceptor.impl.StatisticsSqlInterceptor";
  private String sqlInterceptorType = "select,update,insert,delete,ddl";

  private String userDir = System.getProperty("user.dir");
  private String sqlInterceptorFile = ((userDir.endsWith("/lib") || userDir.endsWith("/bin"))
      ? userDir.substring(0, userDir.length() - 4)
      : userDir) + "/sql/sql.txt";

  public static final int MUTINODELIMIT_SMALL_DATA = 0;
  public static final int MUTINODELIMIT_LAR_DATA = 1;
  private int mutiNodeLimitType = MUTINODELIMIT_SMALL_DATA;

  public static final int MUTINODELIMIT_PATCH_SIZE = 100;
  private int mutiNodePatchSize = MUTINODELIMIT_PATCH_SIZE;

  private String defaultSqlParser = DEFAULT_SQL_PARSER;
  private int usingAIO = 0;
  private int packetHeaderSize = 4;
  private int maxPacketSize = 16 * 1024 * 1024;
  private int dmdsNodeId = 1;
  private int useCompression = 0;
  // 慢SQL的时间阀值
  private long SQL_SLOW_TIME = 1000;

  public String getDefaultSqlParser() {
    return defaultSqlParser;
  }

  public void setDefaultSqlParser(String defaultSqlParser) {
    this.defaultSqlParser = defaultSqlParser;
  }

  public SystemConfig() {
    this.serverPort = DEFAULT_PORT;
    this.managerPort = DEFAULT_MANAGER_PORT;
    this.charset = DEFAULT_CHARSET;
    this.processors = DEFAULT_PROCESSORS;

    this.processorBufferChunk = DEFAULT_BUFFER_CHUNK_SIZE;
    this.processorExecutor = (DEFAULT_PROCESSORS != 1) ? DEFAULT_PROCESSORS * 2 : 4;
    this.managerExecutor = 2;
    this.processorBufferPool = DEFAULT_BUFFER_CHUNK_SIZE * processors * 1000;
    this.processorBufferLocalPercent = 100;
    this.timerExecutor = 2;
    this.idleTimeout = DEFAULT_IDLE_TIMEOUT;
    this.processorCheckPeriod = DEFAULT_PROCESSOR_CHECK_PERIOD;
    this.dataNodeIdleCheckPeriod = DEFAULT_DATANODE_IDLE_CHECK_PERIOD;
    this.dataNodeHeartbeatPeriod = DEFAULT_DATANODE_HEARTBEAT_PERIOD;
    this.clusterHeartbeatUser = DEFAULT_CLUSTER_HEARTBEAT_USER;
    this.clusterHeartbeatPass = DEFAULT_CLUSTER_HEARTBEAT_PASS;
    this.clusterHeartbeatPeriod = DEFAULT_CLUSTER_HEARTBEAT_PERIOD;
    this.clusterHeartbeatTimeout = DEFAULT_CLUSTER_HEARTBEAT_TIMEOUT;
    this.clusterHeartbeatRetry = DEFAULT_CLUSTER_HEARTBEAT_RETRY;
    this.txIsolation = Isolations.REPEATED_READ;
    this.parserCommentVersion = DEFAULT_PARSER_COMMENT_VERSION;
    this.sqlRecordCount = DEFAULT_SQL_RECORD_COUNT;
    this.SQL_SLOW_TIME = 1000;

  }

  public void setSlowTime(long time) {
    this.SQL_SLOW_TIME = time;
  }

  public long getSlowTime() {
    return this.SQL_SLOW_TIME;
  }

  public String getSqlInterceptor() {
    return sqlInterceptor;
  }

  public void setSqlInterceptor(String sqlInterceptor) {
    this.sqlInterceptor = sqlInterceptor;
  }

  public int getPacketHeaderSize() {
    return packetHeaderSize;
  }

  public void setPacketHeaderSize(int packetHeaderSize) {
    this.packetHeaderSize = packetHeaderSize;
  }

  public int getMaxPacketSize() {
    return maxPacketSize;
  }

  public void setMaxPacketSize(int maxPacketSize) {
    this.maxPacketSize = maxPacketSize;
  }

  public int getFrontWriteQueueSize() {
    return frontWriteQueueSize;
  }

  public void setFrontWriteQueueSize(int frontWriteQueueSize) {
    this.frontWriteQueueSize = frontWriteQueueSize;
  }

  public String getBindIp() {
    return bindIp;
  }

  public void setBindIp(String bindIp) {
    this.bindIp = bindIp;
  }

  public int getDefaultMaxLimit() {
    return defaultMaxLimit;
  }

  public void setDefaultMaxLimit(int defaultMaxLimit) {
    this.defaultMaxLimit = defaultMaxLimit;
  }

  public static String getHomePath() {
    String home = System.getProperty(SystemConfig.SYS_HOME);
    if (home != null) {
      if (home.endsWith(File.pathSeparator)) {
        home = home.substring(0, home.length() - 1);
        System.setProperty(SystemConfig.SYS_HOME, home);
      }
    }

    // DMDS_HOME为空，默认尝试设置为当前目录或上级目录。BEN
    if (home == null) {
      try {
        String path = new File("..").getCanonicalPath().replaceAll("\\\\", "/");
        File conf = new File(path + "/conf");
        if (conf.exists() && conf.isDirectory()) {
          home = path;
        } else {
          path = new File(".").getCanonicalPath().replaceAll("\\\\", "/");
          conf = new File(path + "/conf");
          if (conf.exists() && conf.isDirectory()) {
            home = path;
          }
        }

        if (home != null) {
          System.setProperty(SystemConfig.SYS_HOME, home);
        }
      } catch (IOException e) {
        // 如出错，则忽略。
      }
    }

    return home;
  }

  public int getUseCompression() {
    return useCompression;
  }

  public void setUseCompression(int useCompression) {
    this.useCompression = useCompression;
  }

  public String getCharset() {
    return charset;
  }

  public void setCharset(String charset) {
    this.charset = charset;
  }

  public String getFakeMySQLVersion() {
    return fakeMySQLVersion;
  }

  public void setFakeMySQLVersion(String mysqlVersion) {
    this.fakeMySQLVersion = mysqlVersion;
  }

  public int getServerPort() {
    return serverPort;
  }

  public void setServerPort(int serverPort) {
    this.serverPort = serverPort;
  }

  public int getManagerPort() {
    return managerPort;
  }

  public void setManagerPort(int managerPort) {
    this.managerPort = managerPort;
  }

  public int getProcessors() {
    return processors;
  }

  public void setProcessors(int processors) {
    this.processors = processors;
  }

  public int getProcessorExecutor() {
    return processorExecutor;
  }

  public void setProcessorExecutor(int processorExecutor) {
    this.processorExecutor = processorExecutor;
  }

  public int getManagerExecutor() {
    return managerExecutor;
  }

  public void setManagerExecutor(int managerExecutor) {
    this.managerExecutor = managerExecutor;
  }

  public int getTimerExecutor() {
    return timerExecutor;
  }

  public void setTimerExecutor(int timerExecutor) {
    this.timerExecutor = timerExecutor;
  }

  public long getIdleTimeout() {
    return idleTimeout;
  }

  public void setIdleTimeout(long idleTimeout) {
    this.idleTimeout = idleTimeout;
  }

  public long getProcessorCheckPeriod() {
    return processorCheckPeriod;
  }

  public void setProcessorCheckPeriod(long processorCheckPeriod) {
    this.processorCheckPeriod = processorCheckPeriod;
  }

  public long getDataNodeIdleCheckPeriod() {
    return dataNodeIdleCheckPeriod;
  }

  public void setDataNodeIdleCheckPeriod(long dataNodeIdleCheckPeriod) {
    this.dataNodeIdleCheckPeriod = dataNodeIdleCheckPeriod;
  }

  public long getDataNodeHeartbeatPeriod() {
    return dataNodeHeartbeatPeriod;
  }

  public void setDataNodeHeartbeatPeriod(long dataNodeHeartbeatPeriod) {
    this.dataNodeHeartbeatPeriod = dataNodeHeartbeatPeriod;
  }

  public String getClusterHeartbeatUser() {
    return clusterHeartbeatUser;
  }

  public void setClusterHeartbeatUser(String clusterHeartbeatUser) {
    this.clusterHeartbeatUser = clusterHeartbeatUser;
  }

  public long getSqlExecuteTimeout() {
    return sqlExecuteTimeout;
  }

  public void setSqlExecuteTimeout(long sqlExecuteTimeout) {
    this.sqlExecuteTimeout = sqlExecuteTimeout;
  }

  public String getClusterHeartbeatPass() {
    return clusterHeartbeatPass;
  }

  public void setClusterHeartbeatPass(String clusterHeartbeatPass) {
    this.clusterHeartbeatPass = clusterHeartbeatPass;
  }

  public long getClusterHeartbeatPeriod() {
    return clusterHeartbeatPeriod;
  }

  public void setClusterHeartbeatPeriod(long clusterHeartbeatPeriod) {
    this.clusterHeartbeatPeriod = clusterHeartbeatPeriod;
  }

  public long getClusterHeartbeatTimeout() {
    return clusterHeartbeatTimeout;
  }

  public void setClusterHeartbeatTimeout(long clusterHeartbeatTimeout) {
    this.clusterHeartbeatTimeout = clusterHeartbeatTimeout;
  }

  public int getFrontsocketsorcvbuf() {
    return frontSocketSoRcvbuf;
  }

  public int getFrontsocketsosndbuf() {
    return frontSocketSoSndbuf;
  }

  public int getBacksocketsorcvbuf() {
    return backSocketSoRcvbuf;
  }

  public int getBacksocketsosndbuf() {
    return backSocketSoSndbuf;
  }

  public int getClusterHeartbeatRetry() {
    return clusterHeartbeatRetry;
  }

  public void setClusterHeartbeatRetry(int clusterHeartbeatRetry) {
    this.clusterHeartbeatRetry = clusterHeartbeatRetry;
  }

  public int getTxIsolation() {
    return txIsolation;
  }

  public void setTxIsolation(int txIsolation) {
    this.txIsolation = txIsolation;
  }

  public int getParserCommentVersion() {
    return parserCommentVersion;
  }

  public void setParserCommentVersion(int parserCommentVersion) {
    this.parserCommentVersion = parserCommentVersion;
  }

  public int getSqlRecordCount() {
    return sqlRecordCount;
  }

  public void setSqlRecordCount(int sqlRecordCount) {
    this.sqlRecordCount = sqlRecordCount;
  }

  public long getProcessorBufferPool() {
    return processorBufferPool;
  }

  public void setProcessorBufferPool(long processorBufferPool) {
    this.processorBufferPool = processorBufferPool;
  }

  public int getProcessorBufferChunk() {
    return processorBufferChunk;
  }

  public void setProcessorBufferChunk(int processorBufferChunk) {
    this.processorBufferChunk = processorBufferChunk;
  }

  public int getFrontSocketSoRcvbuf() {
    return frontSocketSoRcvbuf;
  }

  public void setFrontSocketSoRcvbuf(int frontSocketSoRcvbuf) {
    this.frontSocketSoRcvbuf = frontSocketSoRcvbuf;
  }

  public int getFrontSocketSoSndbuf() {
    return frontSocketSoSndbuf;
  }

  public void setFrontSocketSoSndbuf(int frontSocketSoSndbuf) {
    this.frontSocketSoSndbuf = frontSocketSoSndbuf;
  }

  public int getBackSocketSoRcvbuf() {
    return backSocketSoRcvbuf;
  }

  public void setBackSocketSoRcvbuf(int backSocketSoRcvbuf) {
    this.backSocketSoRcvbuf = backSocketSoRcvbuf;
  }

  public int getBackSocketSoSndbuf() {
    return backSocketSoSndbuf;
  }

  public void setBackSocketSoSndbuf(int backSocketSoSndbuf) {
    this.backSocketSoSndbuf = backSocketSoSndbuf;
  }

  public int getFrontSocketNoDelay() {
    return frontSocketNoDelay;
  }

  public void setFrontSocketNoDelay(int frontSocketNoDelay) {
    this.frontSocketNoDelay = frontSocketNoDelay;
  }

  public int getBackSocketNoDelay() {
    return backSocketNoDelay;
  }

  public void setBackSocketNoDelay(int backSocketNoDelay) {
    this.backSocketNoDelay = backSocketNoDelay;
  }

  public int getMaxStringLiteralLength() {
    return maxStringLiteralLength;
  }

  public void setMaxStringLiteralLength(int maxStringLiteralLength) {
    this.maxStringLiteralLength = maxStringLiteralLength;
  }

  public int getMutiNodeLimitType() {
    return mutiNodeLimitType;
  }

  public void setMutiNodeLimitType(int mutiNodeLimitType) {
    this.mutiNodeLimitType = mutiNodeLimitType;
  }

  public int getMutiNodePatchSize() {
    return mutiNodePatchSize;
  }

  public void setMutiNodePatchSize(int mutiNodePatchSize) {
    this.mutiNodePatchSize = mutiNodePatchSize;
  }

  public int getProcessorBufferLocalPercent() {
    return processorBufferLocalPercent;
  }

  public void setProcessorBufferLocalPercent(int processorBufferLocalPercent) {
    this.processorBufferLocalPercent = processorBufferLocalPercent;
  }

  public String getSqlInterceptorType() {
    return sqlInterceptorType;
  }

  public void setSqlInterceptorType(String sqlInterceptorType) {
    this.sqlInterceptorType = sqlInterceptorType;
  }

  public String getSqlInterceptorFile() {
    return sqlInterceptorFile;
  }

  public void setSqlInterceptorFile(String sqlInterceptorFile) {
    this.sqlInterceptorFile = sqlInterceptorFile;
  }

  public int getUsingAIO() {
    return usingAIO;
  }

  public void setUsingAIO(int usingAIO) {
    this.usingAIO = usingAIO;
  }

  public int getDmdsNodeId() {
    return dmdsNodeId;
  }

  public void setDmdsNodeId(int dmdsNodeId) {
    this.dmdsNodeId = dmdsNodeId;
  }

  @Override
  public String toString() {
    return "SystemConfig [processorBufferLocalPercent=" + processorBufferLocalPercent
        + ", frontSocketSoRcvbuf="
        + frontSocketSoRcvbuf + ", frontSocketSoSndbuf=" + frontSocketSoSndbuf
        + ", backSocketSoRcvbuf="
        + backSocketSoRcvbuf + ", backSocketSoSndbuf=" + backSocketSoSndbuf
        + ", frontSocketNoDelay="
        + frontSocketNoDelay + ", backSocketNoDelay=" + backSocketNoDelay
        + ", maxStringLiteralLength="
        + maxStringLiteralLength + ", frontWriteQueueSize=" + frontWriteQueueSize + ", bindIp="
        + bindIp
        + ", serverPort=" + serverPort + ", managerPort=" + managerPort + ", charset=" + charset
        + ", processors=" + processors + ", processorExecutor=" + processorExecutor
        + ", timerExecutor="
        + timerExecutor + ", managerExecutor=" + managerExecutor + ", idleTimeout=" + idleTimeout
        + ", sqlExecuteTimeout=" + sqlExecuteTimeout + ", processorCheckPeriod="
        + processorCheckPeriod
        + ", dataNodeIdleCheckPeriod=" + dataNodeIdleCheckPeriod + ", dataNodeHeartbeatPeriod="
        + dataNodeHeartbeatPeriod + ", clusterHeartbeatUser=" + clusterHeartbeatUser
        + ", clusterHeartbeatPass="
        + clusterHeartbeatPass + ", clusterHeartbeatPeriod=" + clusterHeartbeatPeriod
        + ", clusterHeartbeatTimeout=" + clusterHeartbeatTimeout + ", clusterHeartbeatRetry="
        + clusterHeartbeatRetry + ", txIsolation=" + txIsolation + ", parserCommentVersion="
        + parserCommentVersion + ", sqlRecordCount=" + sqlRecordCount + ", processorBufferPool="
        + processorBufferPool + ", processorBufferChunk=" + processorBufferChunk
        + ", defaultMaxLimit="
        + defaultMaxLimit + ", sqlInterceptor=" + sqlInterceptor + ", sqlInterceptorType="
        + sqlInterceptorType
        + ", sqlInterceptorFile=" + sqlInterceptorFile + ", mutiNodeLimitType=" + mutiNodeLimitType
        + ", mutiNodePatchSize=" + mutiNodePatchSize + ", defaultSqlParser=" + defaultSqlParser
        + ", usingAIO="
        + usingAIO + ", packetHeaderSize=" + packetHeaderSize + ", maxPacketSize=" + maxPacketSize
        + ", dmdsNodeId=" + dmdsNodeId + "]";
  }
}
