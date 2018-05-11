/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser.druid;

import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlSelectParser;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;

public class DmdsSelectParser extends MySqlSelectParser {

  public DmdsSelectParser(SQLExprParser exprParser) {
    super(exprParser);
  }

  public DmdsSelectParser(String sql) {
    super(sql);
  }

  @Override
  protected SQLSelectItem parseSelectItem() {
    parseTop();
    return super.parseSelectItem();
  }

  public void parseTop() {
    if (lexer.token() == Token.TOP) {
      lexer.nextToken();

      boolean paren = false;
      if (lexer.token() == Token.LPAREN) {
        paren = true;
        lexer.nextToken();
      }

      if (paren) {
        accept(Token.RPAREN);
      }

      if (lexer.token() == Token.LITERAL_INT) {
        lexer.mark();
        lexer.nextToken();
      }
      if (lexer.token() == Token.IDENTIFIER) {
        lexer.nextToken();

      }
      if (lexer.token() == Token.EQ || lexer.token() == Token.DOT) {
        lexer.nextToken();
      } else if (lexer.token() != Token.STAR) {
        lexer.reset();
      }
      if (lexer.token() == Token.PERCENT) {
        lexer.nextToken();
      }

    }

  }
}
