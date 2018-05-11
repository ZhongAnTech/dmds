/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.manager.interceptor.impl;

import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.commons.util.DateUtil;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.IServerConnection;
import com.zhongan.dmds.core.SQLInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 完善sql信息增加时间和host, 并排除打印'select 1'
 */
public class StatisticsSqlInterceptor implements SQLInterceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsSqlInterceptor.class);

  private static Map<String, Integer> typeMap = new HashMap<String, Integer>();

  static {
    typeMap.put("SELECT", 7);
    typeMap.put("UPDATE", 11);
    typeMap.put("INSERT", 4);
    typeMap.put("DELETE", 3);
    typeMap.put("DDL", 100);
  }

  private final class StatisticsSqlRunner implements Runnable {

    private int sqltype = 0;
    private String sqls = "";
    private IServerConnection sc;

    public StatisticsSqlRunner(int sqltype, String sqls, IServerConnection sc) {
      this.sqltype = sqltype;
      this.sqls = sqls;
      this.sc = sc;
    }

    public void run() {
      try {
        SystemConfig sysconfig = DmdsContext.getInstance().getSystem();
        String sqlInterceptorType = sysconfig.getSqlInterceptorType();
        String sqlInterceptorFile = sysconfig.getSqlInterceptorFile();

        String[] sqlInterceptorTypes = sqlInterceptorType.split(",");
        for (String type : sqlInterceptorTypes) {
          if (StatisticsSqlInterceptor.parseType(type.toUpperCase()) == sqltype) {
            switch (sqltype) {
              case ServerParse.SELECT:
                if (!"SELECT 1".equalsIgnoreCase(sqls)) {
                  StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, getLogContent(sqls, sc));
                }
                break;
              case ServerParse.UPDATE:
                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, getLogContent(sqls, sc));
                break;
              case ServerParse.INSERT:
                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, getLogContent(sqls, sc));
                break;
              case ServerParse.DELETE:
                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, getLogContent(sqls, sc));
                break;
              case ServerParse.DDL:
                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, getLogContent(sqls, sc));
                break;
              default:
                break;
            }
          }
        }

      } catch (Exception e) {
        LOGGER.error("interceptSQL error:" + e.getMessage());
      }
    }
  }

  private static String getLogContent(String sql, IServerConnection sc) {
    return DateUtil.toString(new Date(), DateUtil.TIME_PATTERN) + " " + sc.getHost() + "：" + sql;
  }

  public static int parseType(String type) {
    return typeMap.get(type);
  }

  /**
   * 方法追加文件：使用FileWriter
   */
  private static synchronized void appendFile(String fileName, String content) {
    Calendar calendar = Calendar.getInstance();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    String dayFile = dateFormat.format(calendar.getTime());
    try {
      String newFileName = fileName;
      // 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
      String[] title = newFileName.split("\\.");
      if (title.length == 2) {
        newFileName = title[0] + dayFile + "." + title[1];
      }
      File file = new File(newFileName);
      if (!file.exists()) {
        file.createNewFile();
      }
      FileWriter writer = new FileWriter(file, true);
      String newContent =
          content.replaceAll("[\\t\\n\\r]", "") + System.getProperty("line.separator");
      writer.write(newContent);
      writer.close();
    } catch (IOException e) {
      LOGGER.error("appendFile error:" + e + ",fileName=" + fileName);
    }
  }

  /**
   * interceptSQL , type :insert,delete,update,select exectime:xxx ms log content : select:select 1
   * from table,exectime:100ms,shared:1 etc
   */
  @Override
  public String interceptSQL(String sql, int sqlType, IServerConnection sc) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("sql interceptSQL: \r\n" + sql);
    }
    DmdsContext.getInstance().getBusinessExecutor()
        .execute(new StatisticsSqlRunner(sqlType, sql, sc));
    return DefaultSqlInterceptor.replaceDDLIfNotExists(sql, sqlType);
  }

}
