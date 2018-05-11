/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.util;

import com.zhongan.dmds.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CharsetUtil {

  public static final Logger logger = LoggerFactory.getLogger(CharsetUtil.class);
  private static final Map<Integer, String> INDEX_TO_CHARSET = new HashMap<>();
  private static final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();

  static {
    // index_to_charset.properties
    INDEX_TO_CHARSET.put(1, "big5");
    INDEX_TO_CHARSET.put(8, "latin1");
    INDEX_TO_CHARSET.put(9, "latin2");
    INDEX_TO_CHARSET.put(14, "cp1251");
    INDEX_TO_CHARSET.put(28, "gbk");
    INDEX_TO_CHARSET.put(24, "gb2312");
    INDEX_TO_CHARSET.put(33, "utf8");
    INDEX_TO_CHARSET.put(45, "utf8mb4");

    String filePath =
        System.getProperty(SystemConfig.SYS_HOME) + "/conf/index_to_charset.properties";
    Properties prop = new Properties();
    try {
      prop.load(new FileInputStream(filePath));
      for (Object index : prop.keySet()) {
        INDEX_TO_CHARSET.put(Integer.parseInt((String) index), prop.getProperty((String) index));
      }
    } catch (Throwable e) {
      logger.error("加载字符集出错：{}", e);
    }
    for (int i = 0; i < INDEX_TO_CHARSET.size(); i++) {
      String charset = INDEX_TO_CHARSET.get(i);
      if (charset != null && CHARSET_TO_INDEX.get(charset) == null) {
        CHARSET_TO_INDEX.put(charset, i);
      }
    }
    CHARSET_TO_INDEX.put("iso-8859-1", 14);
    CHARSET_TO_INDEX.put("iso_8859_1", 14);
    CHARSET_TO_INDEX.put("utf-8", 33);
  }

  public static final String getCharset(int index) {
    return INDEX_TO_CHARSET.get(index);
  }

  public static final int getIndex(String charset) {
    if (charset == null || charset.length() == 0) {
      return 0;
    } else {
      Integer i = CHARSET_TO_INDEX.get(charset.toLowerCase());
      return (i == null) ? 0 : i;
    }
  }

}