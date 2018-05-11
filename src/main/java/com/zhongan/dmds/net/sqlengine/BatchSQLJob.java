/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.sqlengine;

import com.zhongan.dmds.core.DmdsContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BatchSQLJob {

  private ConcurrentHashMap<Integer, SQLJob> runningJobs = new ConcurrentHashMap<Integer, SQLJob>();
  private ConcurrentLinkedQueue<SQLJob> waitingJobs = new ConcurrentLinkedQueue<SQLJob>();
  private volatile boolean noMoreJobInput = false;

  public void addJob(SQLJob newJob, boolean parallExecute) {

    if (parallExecute) {
      runJob(newJob);
    } else {
      waitingJobs.offer(newJob);
      if (runningJobs.isEmpty()) {
        SQLJob job = waitingJobs.poll();
        if (job != null) {
          runJob(job);
        }
      }
    }
  }

  public void setNoMoreJobInput(boolean noMoreJobInput) {
    this.noMoreJobInput = noMoreJobInput;
  }

  private void runJob(SQLJob newJob) {
    runningJobs.put(newJob.getId(), newJob);
    DmdsContext.getInstance().getBusinessExecutor().execute(newJob);
  }

  public boolean jobFinished(SQLJob sqlJob) {
    if (EngineCtx.LOGGER.isDebugEnabled()) {
      EngineCtx.LOGGER.info("job finished " + sqlJob);
    }
    runningJobs.remove(sqlJob.getId());
    SQLJob job = waitingJobs.poll();
    if (job != null) {
      runJob(job);
      return false;
    } else {
      if (noMoreJobInput) {
        return runningJobs.isEmpty() && waitingJobs.isEmpty();
      } else {
        return false;
      }
    }

  }
}
