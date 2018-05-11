/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.test;


import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class MySQLTableReplaceUtilTest {

  @Test
  public void testCommon() throws IOException {
    test("tableReplace/test-common.sql");
  }

  @Test
  public void testDDL() throws IOException {
    test("tableReplace/test-ddl.sql");
  }

  @Test
  public void testDML() throws IOException {
    test("tableReplace/test-dml.sql");
  }

  @Test
  public void testDQL() throws IOException {
    test("tableReplace/test-dql.sql");
  }

  private void test(String path) throws IOException {
    List<MySQLTableReplaceTestCase> testCaseList = MySQLTableReplaceTestCaseLoader
        .loadFromResource(path);
    for (MySQLTableReplaceTestCase testCase : testCaseList) {
      Assert.assertTrue(testCase.getName() + " Assert Failed.", testCase.test());
    }
  }

  //    @Test
  public void debug() throws IOException {
    test("tableReplace/debug.sql");
  }

}
