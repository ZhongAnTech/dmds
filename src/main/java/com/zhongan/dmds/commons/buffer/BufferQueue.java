/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.buffer;

import java.nio.ByteBuffer;
import java.util.*;

public final class BufferQueue {

  private final long total;
  private final LinkedList<ByteBuffer> items = new LinkedList<ByteBuffer>();

  public BufferQueue(long capacity) {
    this.total = capacity;
  }

  /**
   * used for statics
   *
   * @return
   */
  public int snapshotSize() {
    return this.items.size();
  }

  public Collection<ByteBuffer> removeItems(long count) {

    List<ByteBuffer> removed = new ArrayList<ByteBuffer>();
    Iterator<ByteBuffer> itor = items.iterator();
    while (itor.hasNext()) {
      removed.add(itor.next());
      itor.remove();
      if (removed.size() >= count) {
        break;
      }
    }
    return removed;
  }

  /**
   * @param buffer
   * @throws InterruptedException
   */
  public void put(ByteBuffer buffer) {
    this.items.offer(buffer);
    if (items.size() > total) {
      throw new java.lang.RuntimeException(
          "bufferQueue size exceeded ,maybe sql returned too many records ,cursize:" + items
              .size());

    }
  }

  public ByteBuffer poll() {
    return items.poll();
  }

  public boolean isEmpty() {
    return items.isEmpty();
  }

}