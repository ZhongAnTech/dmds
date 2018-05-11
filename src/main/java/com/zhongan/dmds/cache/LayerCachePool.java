/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.cache;

import java.util.Map;

/**
 * Layered cache pool
 */
public interface LayerCachePool extends CachePool {

  public void putIfAbsent(String primaryKey, Object secondKey, Object value);

  public Object get(String primaryKey, Object secondKey);

  /**
   * get all cache static, name is cache name
   *
   * @return map of CacheStatic
   */
  public Map<String, CacheStatic> getAllCacheStatic();
}