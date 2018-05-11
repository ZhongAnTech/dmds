/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.parser.Lexer;

/**
 * Base on MycatExprParser
 */
public class DmdsExprParser extends MySqlExprParser {

  public static String[] max_agg_functions = {"AVG", "COUNT", "GROUP_CONCAT", "MAX", "MIN",
      "STDDEV", "SUM",
      "ROW_NUMBER"};

  public DmdsExprParser(Lexer lexer) {
    super(lexer);
    super.aggregateFunctions = max_agg_functions;
  }

  public DmdsExprParser(String sql) {
    super(new DmdsLexer(sql));
    lexer.nextToken();
    super.aggregateFunctions = max_agg_functions;
  }
}
