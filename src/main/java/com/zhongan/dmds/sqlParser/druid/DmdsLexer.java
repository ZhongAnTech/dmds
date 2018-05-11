/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.druid.sql.parser.Keywords;
import com.alibaba.druid.sql.parser.Token;

import java.util.HashMap;
import java.util.Map;

/**
 * Base on MycatLexer
 */
public class DmdsLexer extends MySqlLexer {

  public final static Keywords DEFAULT_DMDS_KEYWORDS;

  static {
    Map<String, Token> map = new HashMap<String, Token>();

    map.putAll(Keywords.DEFAULT_KEYWORDS.getKeywords());

    map.put("DUAL", Token.DUAL);
    map.put("FALSE", Token.FALSE);
    map.put("IDENTIFIED", Token.IDENTIFIED);
    map.put("IF", Token.IF);
    map.put("KILL", Token.KILL);

    map.put("LIMIT", Token.LIMIT);
    map.put("TRUE", Token.TRUE);
    map.put("BINARY", Token.BINARY);
    map.put("SHOW", Token.SHOW);
    map.put("CACHE", Token.CACHE);
    map.put("ANALYZE", Token.ANALYZE);
    map.put("OPTIMIZE", Token.OPTIMIZE);
    map.put("ROW", Token.ROW);
    map.put("BEGIN", Token.BEGIN);
    map.put("END", Token.END);

    map.put("TOP", Token.TOP);

    DEFAULT_DMDS_KEYWORDS = new Keywords(map);
  }

  public DmdsLexer(char[] input, int inputLength, boolean skipComment) {
    super(input, inputLength, skipComment);
    super.keywods = DEFAULT_DMDS_KEYWORDS;
  }

  public DmdsLexer(String input) {
    super(input);
    super.keywods = DEFAULT_DMDS_KEYWORDS;
  }
}
