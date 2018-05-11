/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.cache.impl;

import com.zhongan.dmds.cache.CachePool;
import com.zhongan.dmds.cache.CacheStatic;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 缓存SQLRoute信息
 *
 * @author niuzichun
 * @created Oct 24, 2017
 */
public class EnchachePool implements CachePool {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnchachePool.class);
  private final Cache enCache;
  private final CacheStatic cacheStati = new CacheStatic();
  private final String name;
  private final long maxSize;

  public EnchachePool(String name, Cache enCache, long maxSize) {
    this.enCache = enCache;
    this.name = name;
    this.maxSize = maxSize;
    cacheStati.setMaxSize(this.getMaxSize());
  }

  @Override
  public void putIfAbsent(Object key, Object value) {
    Element el = new Element(key, value);
    if (enCache.putIfAbsent(el) == null) {
      cacheStati.incPutTimes();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(name + " add cache ,key:" + key + " value:" + value);
      }
    }
  }

  @Override
  public Object get(Object key) {
    Element cacheEl = enCache.get(key);
    if (cacheEl != null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(name + " hit cache ,key:" + key);
      }
      cacheStati.incHitTimes();
      return cacheEl.getObjectValue();
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(name + "  miss cache ,key:" + key);
      }
      cacheStati.incAccessTimes();
      return null;
    }
  }

  @Override
  public void clearCache() {
    LOGGER.info("clear cache " + name);
    enCache.removeAll();
    enCache.clearStatistics();
    cacheStati.reset();
    cacheStati.setMemorySize(enCache.getMemoryStoreSize());

  }

  @Override
  public CacheStatic getCacheStatic() {
    cacheStati.setItemSize(enCache.getSize());
    return cacheStati;
  }

  @Override
  public long getMaxSize() {
    return maxSize;
  }

}