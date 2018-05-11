/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser;

import com.zhongan.dmds.commons.parse.ParseUtil;

public final class ManagerParseSelect {

  public static final int OTHER = -1;
  public static final int VERSION_COMMENT = 1;
  public static final int SESSION_AUTO_INCREMENT = 2;
  public static final int SESSION_TX_READ_ONLY = 3;

  private static final char[] _VERSION_COMMENT = "VERSION_COMMENT".toCharArray();
  private static final char[] _SESSION_AUTO_INCREMENT = "SESSION.AUTO_INCREMENT_INCREMENT"
      .toCharArray();
  private static final char[] _SESSION_TX_READ_ONLY = "SESSION.TX_READ_ONLY".toCharArray();

  public static int parse(String stmt, int offset) {
    int i = offset;
    for (; i < stmt.length(); i++) {
      switch (stmt.charAt(i)) {
        case ' ':
          continue;
        case '/':
        case '#':
          i = ParseUtil.comment(stmt, i);
          continue;
        case '@':
          return select2Check(stmt, i);
        default:
          return OTHER;
      }
    }
    return OTHER;
  }

  static int select2Check(String stmt, int offset) {
    if (stmt.length() > ++offset && stmt.charAt(offset) == '@') {
      if (stmt.length() > ++offset) {
        switch (stmt.charAt(offset)) {
          case 'S':
          case 's':
            return select2SCheck(stmt, offset);
          case 'V':
          case 'v':
            return select2VCheck(stmt, offset);
          default:
            return OTHER;
        }
      }
    }
    return OTHER;
  }

  // VERSION_COMMENT
  static int select2VCheck(String stmt, int offset) {
    int length = offset + _VERSION_COMMENT.length;
    if (stmt.length() >= length) {
      if (ParseUtil.compare(stmt, offset, _VERSION_COMMENT)) {
        if (stmt.length() > length && stmt.charAt(length) != ' ') {
          return OTHER;
        }
        return VERSION_COMMENT;
      }
    }
    return OTHER;
  }

  // SESSION.AUTO_INCREMENT_INCREMENT or SESSION.TX_READ_ONLY
  static int select2SCheck(String stmt, int offset) {
    int length = offset + _SESSION_AUTO_INCREMENT.length;
    int length_tx_read_only = offset + _SESSION_TX_READ_ONLY.length;
    if ((stmt.length() >= length) && (ParseUtil.compare(stmt, offset, _SESSION_AUTO_INCREMENT))) {
      if (stmt.length() > length && stmt.charAt(length) != ' ') {
        return OTHER;
      }
      return SESSION_AUTO_INCREMENT;
    } else if ((stmt.length() >= length_tx_read_only) && ParseUtil
        .compare(stmt, offset, _SESSION_TX_READ_ONLY)) {
      if (stmt.length() > length_tx_read_only && stmt.charAt(length_tx_read_only) != ' ') {
        return OTHER;
      }
      return SESSION_TX_READ_ONLY;
    }

    return OTHER;
  }

}