/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.cache;

/**
 * simple cache pool for implement
 */
public interface CachePool {

  public void putIfAbsent(Object key, Object value);

  public Object get(Object key);

  public void clearCache();

  public CacheStatic getCacheStatic();

  public long getMaxSize();
}