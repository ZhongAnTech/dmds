/*
 * Copyright (C) 2016-2020 zhongan.com
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio.handler;

import com.zhongan.dmds.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

/**
 * 优化多表查询
 */
public final class RouteResultsetNodeListener implements PropertyChangeListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(RouteResultsetNodeListener.class);
  private RouteResultsetNode[] nodes;
  private MultiNodeQueryHandler handler;

  public RouteResultsetNodeListener(RouteResultsetNode[] nodes, MultiNodeQueryHandler handler) {
    this.nodes = nodes;
    this.handler = handler;
  }

  @Override
  public void propertyChange(PropertyChangeEvent evt) {
    RouteResultsetNode srcNode = ((RouteResultsetNode) evt.getSource());
//		System.out.println("getSource:"+srcNode.getName()+":"+srcNode.getStatement()+"\n");		
    if ((boolean) evt.getNewValue() == true && (boolean) evt.getOldValue() == false) {
      int j = -1;
      for (int i = 0; i < nodes.length; i++) {
        if (srcNode.equals(nodes[i])) {
          j = i + 1;
          break;
        }
      }
      if (j < nodes.length && j > 0) {
        try {
//					System.out.println("马上要执行的sql："+this.nodes[j].getStatement()+"\n");
          handler.execute(this.nodes[j]);
        } catch (Exception e) {
          LOGGER.error(e.getMessage(), e);
        }
      }
    }

  }

}
