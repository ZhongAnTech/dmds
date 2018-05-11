/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.cache;

/**
 * factory used to create cachePool
 */
public abstract class CachePoolFactory {

  /**
   * create a cache pool instance
   *
   * @param poolName
   * @param cacheSize
   * @param expireSeconds -1 for not expired
   * @return
   */
  public abstract CachePool createCachePool(String poolName, int cacheSize, int expireSeconds);
}