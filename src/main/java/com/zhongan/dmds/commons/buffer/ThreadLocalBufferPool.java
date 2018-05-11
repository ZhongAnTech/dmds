/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.buffer;

public class ThreadLocalBufferPool extends ThreadLocal<BufferQueue> {

  private final long size;

  public ThreadLocalBufferPool(long size) {
    this.size = size;
  }

  protected synchronized BufferQueue initialValue() {
    return new BufferQueue(size);
  }
}
