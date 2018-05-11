/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.util;

import com.zhongan.dmds.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DnPropertyUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(DnPropertyUtil.class);

  /**
   * 加载dnindex.properties属性文件
   *
   * @return 属性文件
   */
  public static Properties loadDnIndexProps() {
    Properties prop = new Properties();
    File file = new File(SystemConfig.getHomePath(),
        "conf" + File.separator + "conf/dnindex.properties");
    if (!file.exists()) {
      return prop;
    }
    FileInputStream filein = null;
    try {
      filein = new FileInputStream(file);
      prop.load(filein);
    } catch (Exception e) {
      LOGGER.warn("load DataNodeIndex err:" + e);
    } finally {
      if (filein != null) {
        try {
          filein.close();
        } catch (IOException e) {
        }
      }
    }
    return prop;
  }

}
