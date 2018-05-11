/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Extract from AbstractConnection
 */
public interface NIOConnection extends ClosableConnection {

  String getCharset();

  boolean setCharset(String charset);

  boolean isSupportCompress();

  void setSupportCompress(boolean isSupportCompress);

  int getCharsetIndex();

  long getIdleTimeout();

  SocketWR getSocketWR();

  void setIdleTimeout(long idleTimeout);

  int getLocalPort();

  String getHost();

  void setHost(String host);

  int getPort();

  void setPort(int port);

  void setLocalPort(int localPort);

  long getId();

  void setId(long id);

  boolean isIdleTimeout();

  NetworkChannel getChannel();

  int getPacketHeaderSize();

  void setPacketHeaderSize(int packetHeaderSize);

  int getMaxPacketSize();

  void setMaxPacketSize(int maxPacketSize);

  long getStartupTime();

  long getLastReadTime();

  void setProcessor(INIOProcessor processor);

  long getLastWriteTime();

  long getNetInBytes();

  long getNetOutBytes();

  int getWriteAttempts();

  INIOProcessor getProcessor();

  ByteBuffer getReadBuffer();

  ByteBuffer allocate();

  void recycle(ByteBuffer buffer);

  void setHandler(NIOHandler handler);

  void handle(byte[] data);

  void register() throws IOException;

  void asynRead() throws IOException;

  void doNextWriteCheck() throws IOException;

  void onReadData(int got) throws IOException;

  void write(byte[] data);

  void write(ByteBuffer buffer);

  ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity, boolean writeSocketIfFull);

  ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer);

  void close(String reason);

  boolean isClosed();

  void idleCheck();

  ConcurrentLinkedQueue<ByteBuffer> getWriteQueue();

  public void cleanup();

  public boolean setCharsetIndex(int ci);
}