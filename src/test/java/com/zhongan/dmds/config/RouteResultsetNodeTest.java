/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config;

import com.zhongan.dmds.commons.parse.ServerParse;
import com.zhongan.dmds.route.RouteResultsetNode;
import org.junit.Assert;
import org.junit.Test;

public class RouteResultsetNodeTest {

  @Test
  public void testNoReplaceTable() {
    String sql = "select f1 from t1 left join t2 on t1.f1 = t2.f1";
    String sql2 = "select f1 from t1 left join t2 on t1.f1 = t2.f1 limit 1";
    RouteResultsetNode rrn = new RouteResultsetNode("dn_gr_policy_00", ServerParse.SELECT, sql);
    Assert.assertTrue(rrn.getStatement().equals(sql));
    rrn.setStatement(sql2);
    Assert.assertTrue(rrn.getStatement().equals(sql2));
  }

  @Test
  public void testReplaceTable() {
    String sql = "select f1 from t1 left join t2 on t1.f1 = t2.f1";
    String sql2 = "select f1 from t1 left join t2 on t1.f1 = t2.f1 limit 1";
    RouteResultsetNode rrn = new RouteResultsetNode("dn_gr_policy_00#T1_0000#T2_0000",
        ServerParse.SELECT, sql);
    Assert.assertTrue(rrn.getStatement()
        .equals("SELECT f1 FROM t1_0000 LEFT JOIN t2_0000 ON t1_0000.f1 = t2_0000.f1"));
    rrn.setStatement(sql2);
    Assert.assertTrue(rrn.getStatement()
        .equals("SELECT f1 FROM t1_0000 LEFT JOIN t2_0000 ON t1_0000.f1 = t2_0000.f1 LIMIT 1"));
  }

  @Test
  public void testHashCodeAndEqual() {
    String sql = "select f1 from t1 left join t2 on t1.f1 = t2.f1";
    RouteResultsetNode rrn1 = new RouteResultsetNode("dn_gr_policy_00#T1_0000#T2_0000",
        ServerParse.SELECT, sql);
    RouteResultsetNode rrn2 = new RouteResultsetNode("dn_gr_policy_00#T1_0000#T2_0000",
        ServerParse.SELECT, sql);
    RouteResultsetNode rrn3 = new RouteResultsetNode("dn_gr_policy_01#T1_0000#T2_0000",
        ServerParse.SELECT, sql);
    RouteResultsetNode rrn4 = new RouteResultsetNode("dn_gr_policy_01#T1_0001#T2_0001",
        ServerParse.SELECT, sql);

    Assert.assertTrue(rrn1.equals(rrn2) && rrn1.hashCode() == rrn2.hashCode());
    Assert.assertTrue(!(rrn1.equals(rrn3) || rrn1.hashCode() == rrn3.hashCode()));
    Assert.assertTrue(!(rrn1.equals(rrn4) || rrn1.hashCode() == rrn4.hashCode()));
  }
}
