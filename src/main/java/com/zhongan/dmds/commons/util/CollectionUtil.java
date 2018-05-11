/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.util;

import java.util.*;

public class CollectionUtil {

  /**
   * @param orig if null, return intersect
   */
  public static Set<? extends Object> intersectSet(Set<? extends Object> orig,
      Set<? extends Object> intersect) {
    if (orig == null) {
      return intersect;
    }
    if (intersect == null || orig.isEmpty()) {
      return Collections.emptySet();
    }
    Set<Object> set = new HashSet<Object>(orig.size());
    for (Object p : orig) {
      if (intersect.contains(p)) {
        set.add(p);
      }
    }
    return set;
  }

  public static boolean isEmpty(Collection<?> collection) {
    return collection == null || collection.isEmpty();
  }

  public static boolean isEmpty(Map<?, ?> map) {
    return map == null || map.isEmpty();
  }
}