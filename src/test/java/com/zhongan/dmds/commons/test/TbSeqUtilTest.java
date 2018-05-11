/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.test;

import com.zhongan.dmds.commons.util.TbSeqUtil;
import org.junit.Assert;
import org.junit.Test;

public class TbSeqUtilTest {


  @Test
  public void replaceDivTableName() {
    String sql = "select f1 from t1 left join t2 on t1.f1 = t2.f1";
    replaceDivTableNameTest(sql, null, sql);

    replaceDivTableNameTest(sql, new String[]{}, sql);

    replaceDivTableNameTest(sql, new String[]{"X_0000"}, sql);

    replaceDivTableNameTest(sql, new String[]{"T1_0000"},
        "SELECT f1 FROM t1_0000 LEFT JOIN t2 ON t1_0000.f1 = t2.f1");

    replaceDivTableNameTest(sql, new String[]{"T1_0000", "T2_0000"},
        "SELECT f1 FROM t1_0000 LEFT JOIN t2_0000 ON t1_0000.f1 = t2_0000.f1");
  }

  private void replaceDivTableNameTest(String sourceSQL, String[] tableNames, String validateSQL) {
    String targetSQL = TbSeqUtil.replaceShardingTableNames(sourceSQL, tableNames);
    Assert.assertTrue("Assert error " + sourceSQL, validateSQL.equals(targetSQL));
  }

}
