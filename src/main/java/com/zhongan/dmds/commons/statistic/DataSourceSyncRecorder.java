/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.statistic;

import com.zhongan.dmds.commons.contants.mysql.DmdsConstants;
import com.zhongan.dmds.commons.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 记录最近3个时段的平均响应时间，默认1，10，30分钟。
 *
 * @author songwie
 */
public class DataSourceSyncRecorder {

  private Map<String, String> records;
  private final List<Record> asynRecords;// value,time
  //	private static final Logger LOGGER = Logger.getLogger("DataSourceSyncRecorder");
  private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceSyncRecorder.class);

  private static final long SWAP_TIME = 24 * 60 * 60 * 1000L;

  // 日期处理
  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private int switchType = 2;

  public DataSourceSyncRecorder() {
    this.records = new HashMap<String, String>();
    this.asynRecords = new LinkedList<Record>();
  }

  public String get() {
    return records.toString();
  }

  public void set(Map<String, String> resultResult, int switchType) {
    try {
      long time = TimeUtil.currentTimeMillis();
      this.switchType = switchType;

      remove(time);

      if (resultResult != null && !resultResult.isEmpty()) {
        this.records = resultResult;
        if (switchType == DmdsConstants.SYN_STATUS_SWITCH_DS) { // slave
          String sencords = resultResult.get("Seconds_Behind_Master");
          long Seconds_Behind_Master = -1;
          if (sencords != null) {
            Seconds_Behind_Master = Long.valueOf(sencords);
          }
          this.asynRecords.add(new Record(TimeUtil.currentTimeMillis(), Seconds_Behind_Master));
        }
        if (switchType == DmdsConstants.CLUSTER_STATUS_SWITCH_DS) {// cluster
          double wsrep_local_recv_queue_avg = Double
              .valueOf(resultResult.get("wsrep_local_recv_queue_avg"));
          this.asynRecords
              .add(new Record(TimeUtil.currentTimeMillis(), wsrep_local_recv_queue_avg));
        }

        return;
      }
    } catch (Exception e) {
      LOGGER.error("record DataSourceSyncRecorder error " + e.getMessage());
    }

  }

  /**
   * 删除超过统计时间段的数据
   */
  private void remove(long time) {
    final List<Record> recordsAll = this.asynRecords;
    while (recordsAll.size() > 0) {
      Record record = recordsAll.get(0);
      if (time >= record.time + SWAP_TIME) {
        recordsAll.remove(0);
      } else {
        break;
      }
    }
  }

  public int getSwitchType() {
    return this.switchType;
  }

  public void setSwitchType(int switchType) {
    this.switchType = switchType;
  }

  public Map<String, String> getRecords() {
    return this.records;
  }

  public List<Record> getAsynRecords() {
    return this.asynRecords;
  }

  public static SimpleDateFormat getSdf() {
    return sdf;
  }

  /**
   *
   */
  public static class Record {

    private Object value;
    private long time;

    Record(long time, Object value) {
      this.time = time;
      this.value = value;
    }

    public Object getValue() {
      return this.value;
    }

    public void setValue(Object value) {
      this.value = value;
    }

    public long getTime() {
      return this.time;
    }

    public void setTime(long time) {
      this.time = time;
    }

  }
}
