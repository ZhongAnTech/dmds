/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.parse;

public class ArrayUtil {

  public static boolean equals(String str1, String str2) {
    if (str1 == null) {
      return str2 == null;
    }
    return str1.equals(str2);
  }

  public static boolean contains(String[] list, String str) {
    if (list == null) {
      return false;
    }
    for (String string : list) {
      if (equals(str, string)) {
        return true;
      }
    }
    return false;
  }

}