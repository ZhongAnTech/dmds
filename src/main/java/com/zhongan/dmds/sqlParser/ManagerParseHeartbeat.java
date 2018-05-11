/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.sqlParser;

import com.zhongan.dmds.commons.parse.Pair;

public final class ManagerParseHeartbeat {

  public static final int OTHER = -1;
  public static final int DATASOURCE = 1;

  // SHOW @@HEARTBEAT
  static int show2HeaCheck(String stmt, int offset) {
    if (stmt.length() > offset + "RTBEAT".length()) {
      char c1 = stmt.charAt(++offset);
      char c2 = stmt.charAt(++offset);
      char c3 = stmt.charAt(++offset);
      char c4 = stmt.charAt(++offset);
      char c5 = stmt.charAt(++offset);
      char c6 = stmt.charAt(++offset);
      if ((c1 == 'R' || c1 == 'r') && (c2 == 'T' || c2 == 't') & (c3 == 'B' || c3 == 'b')
          && (c4 == 'E' || c4 == 'e') & (c5 == 'A' || c5 == 'a') && (c6 == 'T' || c6 == 't')) {
        if (stmt.length() > offset + ".DETAIL".length()) {
          char c7 = stmt.charAt(++offset);
          if (c7 == '.') {
            return show2HeaDetailCheck(stmt, offset);
          }
        }
        if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
          return OTHER;
        }
        return ManagerParseShow.HEARTBEAT;
      }
    }
    return OTHER;
  }

  // SHOW @@HEARTBEAT.DETAIL
  static int show2HeaDetailCheck(String stmt, int offset) {
    if (stmt.length() > offset + "DETAIL".length()) {
      char c1 = stmt.charAt(++offset);
      char c2 = stmt.charAt(++offset);
      char c3 = stmt.charAt(++offset);
      char c4 = stmt.charAt(++offset);
      char c5 = stmt.charAt(++offset);
      char c6 = stmt.charAt(++offset);
      if ((c1 == 'D' || c1 == 'd') && (c2 == 'E' || c2 == 'e') & (c3 == 'T' || c3 == 't')
          && (c4 == 'A' || c4 == 'a') & (c5 == 'I' || c5 == 'i') && (c6 == 'L' || c6 == 'l')) {
        if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
          return OTHER;
        }
        return ManagerParseShow.HEARTBEAT_DETAIL;
      }
    }
    return OTHER;
  }

  public static Pair<String, String> getPair(String stmt) {
    int offset = stmt.indexOf("@@");
    String s = stmt.substring(++offset + " heartbeat.detail".length());
    char c = s.charAt(0);
    offset = 0;
    if (c == ' ') {
      char c1 = s.charAt(++offset);
      char c2 = s.charAt(++offset);
      char c3 = s.charAt(++offset);
      char c4 = s.charAt(++offset);
      char c5 = s.charAt(++offset);
      char c6 = s.charAt(++offset);
      char c7 = s.charAt(++offset);
      char c8 = s.charAt(++offset);
      char c9 = s.charAt(++offset);
      char c10 = s.charAt(++offset);
      char c11 = s.charAt(++offset);
      if ((c1 == 'W' || c1 == 'w') && (c2 == 'H' || c2 == 'h') && (c3 == 'E' || c3 == 'e')
          && (c4 == 'R' || c4 == 'r') && (c5 == 'E' || c5 == 'e') && c6 == ' ' && (c7 == 'N'
          || c7 == 'n')
          && (c8 == 'A' || c8 == 'a') && (c9 == 'M' || c9 == 'm') && (c10 == 'E' || c10 == 'e')
          && (c11 == '=')) {
        String name = s.substring(++offset).trim();
        return new Pair<String, String>("name", name);
      }
    }
    return new Pair<String, String>("name", "");
  }

}