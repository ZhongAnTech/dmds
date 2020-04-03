/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.parse;

public class CharTypes {

  private final static boolean[] hexFlags = new boolean[256];

  static {
    for (char c = 0; c < hexFlags.length; ++c) {
      if (c >= 'A' && c <= 'F') {
        hexFlags[c] = true;
      } else if (c >= 'a' && c <= 'f') {
        hexFlags[c] = true;
      } else if (c >= '0' && c <= '9') {
        hexFlags[c] = true;
      }
    }
  }

  public static boolean isHex(char c) {
    return c < 256 && hexFlags[c];
  }

  public static boolean isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  private final static boolean[] identifierFlags = new boolean[256];

  static {
    for (char c = 0; c < identifierFlags.length; ++c) {
      if (c >= 'A' && c <= 'Z') {
        identifierFlags[c] = true;
      } else if (c >= 'a' && c <= 'z') {
        identifierFlags[c] = true;
      } else if (c >= '0' && c <= '9') {
        identifierFlags[c] = true;
      }
    }
    // identifierFlags['`'] = true;
    identifierFlags['_'] = true;
    identifierFlags['$'] = true;
  }

  public static boolean isIdentifierChar(char c) {
    return c > identifierFlags.length || identifierFlags[c];
  }

  private final static boolean[] whitespaceFlags = new boolean[256];

  static {
    whitespaceFlags[' '] = true;
    whitespaceFlags['\n'] = true;
    whitespaceFlags['\r'] = true;
    whitespaceFlags['\t'] = true;
    whitespaceFlags['\f'] = true;
    whitespaceFlags['\b'] = true;
  }

  /**
   * @return false if {@link MySQLLexer#EOI}
   */
  public static boolean isWhitespace(char c) {
    return c <= whitespaceFlags.length && whitespaceFlags[c];
  }

}