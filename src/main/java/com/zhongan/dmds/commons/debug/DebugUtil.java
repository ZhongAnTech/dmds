/*
 * Copyright (C) 2016-2020 zhongan.com
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.debug;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(DebugUtil.class);

  public static String trace(Object obj) {
    Exception a = new Exception();
    String res =
        obj.getClass().getSimpleName() + "." + a.getStackTrace()[1].getMethodName() + ":" + JSON
            .toJSONString(obj);
    for (StackTraceElement stat : a.getStackTrace()) {
      LOGGER.debug(stat.getClassName() + ":" + stat.getMethodName() + "_" + stat.getLineNumber());
    }
    LOGGER.debug(res);
    return res;
  }

}
