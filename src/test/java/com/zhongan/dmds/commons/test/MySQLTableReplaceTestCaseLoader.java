/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class MySQLTableReplaceTestCaseLoader {

  public static List<MySQLTableReplaceTestCase> loadFromResource(String path) throws IOException {
    List<MySQLTableReplaceTestCase> testCaseList = new ArrayList<>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(Class.class
        .getResourceAsStream("/" + path), "utf-8"));
    String[] valueArray = new String[4];
    int valueIndex = 0;
    int lineCount = 0;
    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
      lineCount++;
      if (line.trim().isEmpty()) {
        continue;
      }
      line = line.trim();
      switch (valueIndex) {
        case 0:
          if (line.startsWith("--")) {
            valueArray[0] = line.replace("--", "").trim();
            valueIndex++;
            valueArray[valueIndex] = null;
            break;
          }
        case 1:
          if (!line.startsWith("--")) {
            if (valueArray[1] == null) {
              valueArray[1] = line;
            } else {
              valueArray[1] = valueArray[1] + "\n" + line;
            }
            break;
          } else if (valueArray[1] != null) {
            valueIndex++;
            valueArray[valueIndex] = null;
            valueArray[2] = line.replace("--", "").trim();
            valueIndex++;
            valueArray[valueIndex] = null;
            break;
          }
        case 3:
          if (!line.startsWith("--")) {
            if (valueArray[3] == null) {
              valueArray[3] = line;
            } else {
              valueArray[3] = valueArray[3] + "\n" + line;
            }
            break;
          } else if (valueArray[3] != null) {
            testCaseList.add(MySQLTableReplaceTestCase.of(
                valueArray[0], valueArray[1], valueArray[2], valueArray[3]));
            valueIndex = 0;
            valueArray[0] = line.replace("--", "").trim();
            valueIndex++;
            valueArray[valueIndex] = null;
            break;
          }
        default:
          throw new IllegalArgumentException(
              String.format("line count: %s , valueIndex : %s , value %s",
                  lineCount, valueIndex, line));
      }
    }

    if (valueIndex == 3) {
      testCaseList.add(MySQLTableReplaceTestCase.of(
          valueArray[0], valueArray[1], valueArray[2], valueArray[3]));
    } else {
      throw new IllegalArgumentException(String.format("line count: %s , valueIndex : %s",
          lineCount, valueIndex));
    }
    return testCaseList;
  }
}
