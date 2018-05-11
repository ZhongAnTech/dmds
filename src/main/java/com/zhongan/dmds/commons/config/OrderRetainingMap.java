/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.config;

import java.util.*;

public class OrderRetainingMap<K, V> extends HashMap<K, V> {

  private static final long serialVersionUID = 1L;

  private Set<K> keyOrder = new ArraySet<K>();
  private List<V> valueOrder = new ArrayList<V>();

  @Override
  public V put(K key, V value) {
    keyOrder.add(key);
    valueOrder.add(value);
    return super.put(key, value);
  }

  @Override
  public Collection<V> values() {
    return Collections.unmodifiableList(valueOrder);
  }

  @Override
  public Set<K> keySet() {
    return Collections.unmodifiableSet(keyOrder);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    throw new UnsupportedOperationException();
  }

  private static class ArraySet<T> extends ArrayList<T> implements Set<T> {

    private static final long serialVersionUID = 1L;
  }

}