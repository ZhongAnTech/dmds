/*
 * Copyright (C) 2016-2020 zhongan.com
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.commons.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * 提供日期的加减转换等功能 包含多数常用的日期格式
 *
 * @author king
 */
public class DateUtil {

  private static final Logger logger = LoggerFactory.getLogger(DateUtil.class);

  static GregorianCalendar cldr = new GregorianCalendar();

  public static final long DAYMILLI = 24 * 60 * 60 * 1000;

  public static final long HOURMILLI = 60 * 60 * 1000;

  public static final long MINUTEMILLI = 60 * 1000;

  public static final long SECONDMILLI = 1000;

  static {
    cldr.setTimeZone(java.util.TimeZone.getTimeZone("GMT+9:00"));
  }

  public static final transient int BEFORE = 1;

  public static final transient int AFTER = 2;

  public static final transient int EQUAL = 3;

  public static final String TIME_PATTERN_LONG = "dd/MMM/yyyy:HH:mm:ss +0900";

  public static final String TIME_PATTERN_LONG2 = "dd/MM/yyyy:HH:mm:ss +0900";

  public static final String TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

  public static final String DB_TIME_PATTERN = "YYYY-MM-DD HH24:MI:SS";

  public static final String TIME_PATTERN_SHORT = "dd/MM/yy HH:mm:ss";

  public static final String TIME_PATTERN_SHORT_1 = "yyyy/MM/dd HH:mm";

  public static final String TIME_PATTERN_SHORT_2 = "yyyy年MM月dd日 HH:mm:ss";

  public static final String TIME_PATTERN_SESSION = "yyyyMMddHHmmss";

  public static final String DATE_FMT_0 = "yyyyMMdd";

  public static final String DATE_FMT_1 = "yyyy/MM/dd";

  public static final String DATE_FMT_2 = "yyyy/MM/dd HH:mm:ss";

  public static final String DATE_FMT_3 = "yyyy-MM-dd";

  public static final String DATE_FMT_4 = "yyyy年MM月dd日";

  public static final String DATE_FMT_5 = "yyyy-MM-dd HH";

  public static final String DATE_FMT_6 = "yyyy/MM";

  /**
   * change string to date 将String类型的日期转成Date类型
   *
   * @param sDate the date string
   * @param sFmt  the date format
   * @return Date object
   */
  public static java.util.Date toDate(String sDate, String sFmt) {
    if (StringUtils.isBlank(sDate) || StringUtils.isBlank(sFmt)) {
      return null;
    }

    SimpleDateFormat sdfFrom = null;
    java.util.Date dt = null;
    try {
      sdfFrom = new SimpleDateFormat(sFmt);
      dt = sdfFrom.parse(sDate);
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
      return null;
    } finally {
      sdfFrom = null;
    }

    return dt;
  }

  /**
   * change date to string 将日期类型的参数转成String类型
   *
   * @param dt a date
   * @return the format string
   */
  public static String toString(java.util.Date dt) {
    return toString(dt, DATE_FMT_0);
  }

  /**
   * change date object to string 将String类型的日期转成Date类型
   *
   * @param dt   date object
   * @param sFmt the date format
   * @return the formatted string
   */
  public static String toString(java.util.Date dt, String sFmt) {
    if (null == dt || StringUtils.isBlank(sFmt)) {
      return null;
    }

    SimpleDateFormat sdfFrom = null;
    String sRet = null;
    try {
      sdfFrom = new SimpleDateFormat(sFmt);
      sRet = sdfFrom.format(dt).toString();
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
      return null;
    } finally {
      sdfFrom = null;
    }

    return sRet;
  }

  /**
   * 获取Date所属月的最后一天日期
   *
   * @param date
   * @return Date 默认null
   */
  public static Date getMonthLastDate(Date date) {
    if (null == date) {
      return null;
    }

    Calendar ca = Calendar.getInstance();
    ca.setTime(date);
    ca.set(Calendar.HOUR_OF_DAY, 23);
    ca.set(Calendar.MINUTE, 59);
    ca.set(Calendar.SECOND, 59);
    ca.set(Calendar.DAY_OF_MONTH, 1);
    ca.add(Calendar.MONTH, 1);
    ca.add(Calendar.DAY_OF_MONTH, -1);

    Date lastDate = ca.getTime();
    return lastDate;
  }

  /**
   * 获取Date所属月的最后一天日期
   *
   * @param date
   * @param pattern
   * @return String 默认null
   */
  public static String getMonthLastDate(Date date, String pattern) {
    Date lastDate = getMonthLastDate(date);
    if (null == lastDate) {
      return null;
    }

    if (StringUtils.isBlank(pattern)) {
      pattern = TIME_PATTERN;
    }

    return toString(lastDate, pattern);
  }

  /**
   * 获取Date所属月的第一天日期
   *
   * @param date
   * @return Date 默认null
   */
  public static Date getMonthFirstDate(Date date) {
    if (null == date) {
      return null;
    }

    Calendar ca = Calendar.getInstance();
    ca.setTime(date);
    ca.set(Calendar.HOUR_OF_DAY, 0);
    ca.set(Calendar.MINUTE, 0);
    ca.set(Calendar.SECOND, 0);
    ca.set(Calendar.DAY_OF_MONTH, 1);

    Date firstDate = ca.getTime();
    return firstDate;
  }

  /**
   * 获取Date所属月的第一天日期
   *
   * @param date
   * @param pattern
   * @return String 默认null
   */
  public static String getMonthFirstDate(Date date, String pattern) {
    Date firstDate = getMonthFirstDate(date);
    if (null == firstDate) {
      return null;
    }

    if (StringUtils.isBlank(pattern)) {
      pattern = TIME_PATTERN;
    }

    return toString(firstDate, pattern);
  }

  /**
   * 计算两个日期间隔的天数
   *
   * @param firstDate 小者
   * @param lastDate  大者
   * @return int 默认-1
   */
  public static int getIntervalDays(java.util.Date firstDate, java.util.Date lastDate) {
    if (null == firstDate || null == lastDate) {
      return -1;
    }

    long intervalMilli = lastDate.getTime() - firstDate.getTime();
    int diff = (int) (intervalMilli / (24 * 60 * 60 * 1000));
    int mod = (int) (intervalMilli % (24 * 60 * 60 * 1000));
    if (mod > 0) {
      diff = diff + 1;
    }
    return diff;
    // return (int) (intervalMilli / (24 * 60 * 60 * 1000));
  }

  /**
   * 计算两个日期间隔的小时数
   *
   * @param firstDate 小者
   * @param lastDate  大者
   * @return int 默认-1
   */
  public static int getTimeIntervalHours(Date firstDate, Date lastDate) {
    if (null == firstDate || null == lastDate) {
      return -1;
    }

    long intervalMilli = lastDate.getTime() - firstDate.getTime();
    return (int) (intervalMilli / (60 * 60 * 1000));
  }

  /**
   * 计算两个日期间隔的分钟数
   *
   * @param firstDate 小者
   * @param lastDate  大者
   * @return int 默认-1
   */
  public static int getTimeIntervalMins(Date firstDate, Date lastDate) {
    if (null == firstDate || null == lastDate) {
      return -1;
    }

    long intervalMilli = lastDate.getTime() - firstDate.getTime();
    return (int) (intervalMilli / (60 * 1000));
  }

  /**
   * format the date in given pattern 格式化日期
   *
   * @param d       date
   * @param pattern time pattern
   * @return the formatted string
   */
  public static String formatDate(java.util.Date d, String pattern) {
    if (null == d || StringUtils.isBlank(pattern)) {
      return null;
    }

    SimpleDateFormat formatter = (SimpleDateFormat) DateFormat.getDateInstance();

    formatter.applyPattern(pattern);
    return formatter.format(d);
  }

  /**
   * 比较两个日期的先后顺序
   *
   * @param first  date1
   * @param second date2
   * @return EQUAL - if equal BEFORE - if before than date2 AFTER - if over than date2
   */
  public static int compareTwoDate(Date first, Date second) {
    if ((first == null) && (second == null)) {
      return EQUAL;
    } else if (first == null) {
      return BEFORE;
    } else if (second == null) {
      return AFTER;
    } else if (first.before(second)) {
      return BEFORE;
    } else if (first.after(second)) {
      return AFTER;
    } else {
      return EQUAL;
    }
  }

  /**
   * 比较日期是否介于两者之间
   *
   * @param date  the specified date
   * @param begin date1
   * @param end   date2
   * @return true - between date1 and date2 false - not between date1 and date2
   */
  public static boolean isDateBetween(Date date, Date begin, Date end) {
    int c1 = compareTwoDate(begin, date);
    int c2 = compareTwoDate(date, end);

    return (((c1 == BEFORE) && (c2 == BEFORE)) || (c1 == EQUAL) || (c2 == EQUAL));
  }

  /**
   * 比较日期是否介于当前日期的前后数天内
   *
   * @param myDate
   * @param begin
   * @param end
   * @return
   */
  public static boolean isDateBetween(java.util.Date myDate, int begin, int end) {
    return isDateBetween(myDate, getCurrentDateTime(), begin, end);
  }

  /**
   * 比较日期是否介于指定日期的前后数天内
   *
   * @param utilDate
   * @param dateBaseLine
   * @param begin
   * @param end
   * @return
   */
  public static boolean isDateBetween(java.util.Date utilDate, java.util.Date dateBaseLine,
      int begin, int end) {
    String pattern = TIME_PATTERN;

    String my = toString(utilDate, pattern);
    Date myDate = toDate(my, pattern);

    String baseLine = toString(dateBaseLine, pattern);

    // Date baseLineDate = parseString2Date(baseLine, pattern);
    String from = addDays(baseLine, begin);
    Date fromDate = toDate(from, pattern);

    String to = addDays(baseLine, end);
    Date toDate = toDate(to, pattern);

    return isDateBetween(myDate, fromDate, toDate);
  }

  /**
   * 增加天数
   *
   * @param date
   * @param day
   * @return Date
   */
  public static java.util.Date addDate(java.util.Date date, int day) {
    if (null == date) {
      return null;
    }
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) + day);
    return calendar.getTime();
  }

  /**
   * 增加小时
   *
   * @param date
   * @param hour
   * @return
   */
  public static java.util.Date addHour(java.util.Date date, int hour) {
    if (null == date) {
      return null;
    }
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) + hour);
    return calendar.getTime();
  }

  /**
   * 增加分钟
   *
   * @param date
   * @param hour
   * @return
   */
  public static java.util.Date AddMinutes(java.util.Date date, int minute) {
    if (null == date) {
      return null;
    }
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) + minute);
    return calendar.getTime();
  }

  /**
   * 增加年份
   *
   * @param date
   * @param year
   * @return
   */
  public static java.util.Date addYear(java.util.Date date, int year) {
    if (null == date) {
      return null;
    }
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.YEAR, year);
    return calendar.getTime();
  }

  /**
   * 增加天、小时、分钟、秒
   *
   * @param dt  日期
   * @param day 天
   * @param hh  小时
   * @param mm  分钟
   * @param ss  秒
   * @return
   */
  public static Date addDate(Date dt, int day, int hh, int mm, int ss) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(dt);
    cal.add(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.HOUR_OF_DAY, hh);
    cal.set(Calendar.MINUTE, mm);
    cal.set(Calendar.SECOND, ss);
    return cal.getTime();
  }

  /**
   * 根据用户生日计算年龄
   */
  public static int getAge(Date birthday) {
    Calendar cal = Calendar.getInstance();

    if (cal.before(birthday)) {
      throw new IllegalArgumentException("The birthDay is before Now.It's unbelievable!");
    }

    int yearNow = cal.get(Calendar.YEAR);
    int monthNow = cal.get(Calendar.MONTH) + 1;
    int dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);

    cal.setTime(birthday);
    int yearBirth = cal.get(Calendar.YEAR);
    int monthBirth = cal.get(Calendar.MONTH) + 1;
    int dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);

    int age = yearNow - yearBirth;

    if (monthNow <= monthBirth) {
      if (monthNow == monthBirth) {
        // monthNow==monthBirth
        if (dayOfMonthNow < dayOfMonthBirth) {
          age--;
        }
      } else {
        age--;
      }
    }

    return age;
  }

  /**
   * 增加月份
   *
   * @param date
   * @param hour
   * @return
   */
  public static java.util.Date addMonth(java.util.Date date, int month) {
    if (null == date) {
      return null;
    }
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.MONTH, month);
    return calendar.getTime();
  }

  /**
   * 增加天数
   *
   * @param date
   * @param day
   * @param pattern
   * @return
   */
  public static String addDays(java.util.Date date, int day, String pattern) {
    return addDays(toString(date, pattern), day, pattern);
  }

  /**
   * 增加天数
   *
   * @param date
   * @param day
   * @return
   */
  public static String addDays(java.util.Date date, int day) {
    return addDays(toString(date, TIME_PATTERN), day);
  }

  /**
   * 增加天数
   *
   * @param date
   * @param day
   * @return
   */
  public static String addDays(String date, int day) {
    return addDays(date, day, TIME_PATTERN);
  }

  /**
   * get the time of the specified date after given days
   *
   * @param dateStr the specified date
   * @param day     day distance
   * @return the format string of time
   */
  public static String addDays(String dateStr, int day, String pattern) {
    if (StringUtils.isBlank(dateStr)) {
      return "";
    }

    if (day == 0) {
      return dateStr;
    }

    try {
      SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
      Calendar calendar = dateFormat.getCalendar();

      calendar.setTime(dateFormat.parse(dateStr));
      calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) + day);
      return dateFormat.format(calendar.getTime());
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
      return "";
    }
  }

  /**
   * change timestamp to formatted string
   *
   * @param t    Timestamp
   * @param sFmt date format
   * @return formatted string
   */
  public static String formatTimestamp(Timestamp t, String sFmt) {
    if (StringUtils.isBlank(sFmt)) {
      return "";
    }
    t.setNanos(0);
    DateFormat ft = new SimpleDateFormat(sFmt);
    String str = null;
    str = ft.format(t);
    return str;
  }

  /**
   * return current date
   *
   * @return current date
   */
  public static Date getCurrentDate() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    String dateStr = sdf.format(new Date());
    try {
      return sdf.parse(dateStr);
    } catch (ParseException e) {
      throw new RuntimeException("格式化日期出错：", e);
    }
  }

  /**
   * return current calendar instance
   *
   * @return Calendar
   */
  public static Calendar getCurrentCalendar() {
    return Calendar.getInstance();
  }

  public static Calendar getCurrentCalendar(TimeZone utc) {
    return Calendar.getInstance(utc);
  }

  /**
   * return current time
   *
   * @return current time
   */
  public static Timestamp getCurrentDateTime() {
    return new Timestamp(System.currentTimeMillis());
  }

  /**
   * 获取年份
   *
   * @param date Date
   * @return int
   */
  public static final int getYear(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    return calendar.get(Calendar.YEAR);
  }

  /**
   * 获取年份
   *
   * @param millis long
   * @return int
   */
  public static final int getYear(long millis) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(millis);
    return calendar.get(Calendar.YEAR);
  }

  /**
   * 获取月份
   *
   * @param date Date
   * @return int
   */
  public static final int getMonth(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    return calendar.get(Calendar.MONTH) + 1;
  }

  /**
   * 获取月份
   *
   * @param millis long
   * @return int
   */
  public static final int getMonth(long millis) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(millis);
    return calendar.get(Calendar.MONTH) + 1;
  }

  /**
   * 获取日期
   *
   * @param date Date
   * @return int
   */
  public static final int getDate(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    return calendar.get(Calendar.DATE);
  }

  /**
   * 获取日期
   *
   * @param millis long
   * @return int
   */
  public static final int getDate(long millis) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(millis);
    return calendar.get(Calendar.DATE);
  }

  /**
   * 获取小时
   *
   * @param date Date
   * @return int
   */
  public static final int getHour(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    return calendar.get(Calendar.HOUR_OF_DAY);
  }

  /**
   * 获取分钟
   *
   * @param date Date
   * @return int
   */
  public static final int getMinute(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    return calendar.get(Calendar.MINUTE);
  }

  /**
   * 获取秒
   *
   * @param date Date
   * @return int
   */
  public static final int getSecond(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    return calendar.get(Calendar.SECOND);
  }

  /**
   * 获取小时
   *
   * @param millis long
   * @return int
   */
  public static final int getHour(long millis) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(millis);
    return calendar.get(Calendar.HOUR_OF_DAY);
  }

  /**
   * 把日期后的时间归0 变成(yyyy-MM-dd 00:00:00:000)
   *
   * @param date Date
   * @return Date
   */
  public static final Date zerolizedTime(Date fullDate) {
    Calendar cal = Calendar.getInstance();

    cal.setTime(fullDate);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  /**
   * 把日期的时间变成(yyyy-MM-dd 23:59:59:999)
   *
   * @param date
   * @return
   */
  public static final Date getEndTime(Date date) {
    Calendar cal = Calendar.getInstance();

    cal.setTime(date);
    cal.set(Calendar.HOUR_OF_DAY, 23);
    cal.set(Calendar.MINUTE, 59);
    cal.set(Calendar.SECOND, 59);
    cal.set(Calendar.MILLISECOND, 999);
    return cal.getTime();
  }

  public static String getTimeFrom(Date date) {
    String format = "";
    if (date != null) {
      SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      format = sim.format(date);
    }
    return format;
  }

  /**
   * 获取日期对应周一的时间
   *
   * @param date
   * @return
   */
  public static Date getMondayOfWeek(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setFirstDayOfWeek(Calendar.MONDAY);
    calendar.setTime(date);
    calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
    return zerolizedTime(calendar.getTime());
  }

  /**
   * 获取周一的时间
   *
   * @param date
   * @return
   */
  public static Date getMondayOfWeek() {
    Calendar calendar = Calendar.getInstance();
    calendar.setFirstDayOfWeek(Calendar.MONDAY);
    calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
    return zerolizedTime(calendar.getTime());
  }

  /**
   * 获取指定日期是星期几
   *
   * @param date
   * @return
   */
  public static int getDayOfWeek(Date date) {
    Calendar c1 = Calendar.getInstance();
    c1.setTime(date);
    return c1.get(Calendar.DAY_OF_WEEK);
  }

  /**
   * 获取日期对应周日的时间
   *
   * @param date
   * @return
   */
  public static Date getSundayOfWeek(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setFirstDayOfWeek(Calendar.MONDAY);
    calendar.setTime(date);
    calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
    return zerolizedTime(calendar.getTime());
  }

  /**
   * 获取周日的时间
   *
   * @param date
   * @return
   */
  public static Date getSundayOfWeek() {
    Calendar calendar = Calendar.getInstance();
    calendar.setFirstDayOfWeek(Calendar.MONDAY);
    calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
    return zerolizedTime(calendar.getTime());
  }

  /**
   * 格式化操作时间 在0-1分钟之内，显示 'n秒前' 在0-1小时之内，显示 'n分钟前' 在1-24小时之内，显示 'n小时前' 在1天-7天，显示 'n天前' 在7天后的，显示正常日期
   * 'yyyy-MM-dd'
   *
   * @param date 操作时间
   * @return
   */
  public static String format(Date operDate) {
    DateFormat dateFormat = new SimpleDateFormat(DATE_FMT_3);

    // 操作时间
    Calendar operCal = Calendar.getInstance();
    operCal.setTime(operDate);
    // 操作时间1分钟后时间
    Calendar min1Cal = Calendar.getInstance();
    min1Cal.setTime(operDate);
    min1Cal.add(Calendar.MINUTE, 1);
    // 操作时间1小时后时间
    Calendar hour1Cal = Calendar.getInstance();
    hour1Cal.setTime(operDate);
    hour1Cal.add(Calendar.HOUR, 1);
    // 操作时间24小时后时间
    Calendar hour24Cal = Calendar.getInstance();
    hour24Cal.setTime(operDate);
    hour24Cal.add(Calendar.DATE, 1);
    // 操作时间7天后时间
    Calendar day7Cal = Calendar.getInstance();
    day7Cal.setTime(operDate);
    day7Cal.add(Calendar.DATE, 7);
    // 当前时间
    Calendar curCal = Calendar.getInstance();

    long diff = curCal.getTimeInMillis() - operCal.getTimeInMillis();
    // if (curCal.compareTo(operCal) >= 0 && curCal.compareTo(min1Cal) < 0) {
    // return (diff / SECONDMILLI) + "秒前";
    // }
    // 0-1小时之内
    /* else */
    if (curCal.compareTo(operCal) >= 0 && curCal.compareTo(hour1Cal) < 0) {
      long time = (diff / MINUTEMILLI);
      return time > 0 ? (diff / MINUTEMILLI) + "分钟前" : "刚刚";
    }
    // 1-24小时
    else if (curCal.compareTo(hour1Cal) >= 0 && curCal.compareTo(hour24Cal) < 0) {
      return (diff / HOURMILLI) + "小时前";
    }
    // 1天-7天
    else if (curCal.compareTo(hour24Cal) >= 0 && curCal.compareTo(day7Cal) < 0) {
      return (diff / DAYMILLI) + "天前";
    }
    // 大于7天或者当前时间小于操作时间
    else {
      return dateFormat.format(operDate);
    }
  }

  /**
   * 将字符串时间转换成Date类型
   *
   * @param str
   * @return
   */
  public static String convertDate(Date date) {
    if (date != null) {
      SimpleDateFormat sdf = new SimpleDateFormat(DATE_FMT_3);
      return sdf.format(date);
    }
    return null;
  }

  /**
   * 格式化日期字符串 yyyy-MM-dd
   *
   * @param dateStr
   * @return
   */

  public static Date parseDate(String dateStr) {
    if (StringUtils.isNotBlank(dateStr)) {
      SimpleDateFormat sdf = new SimpleDateFormat(DATE_FMT_3);
      try {
        return sdf.parse(dateStr);
      } catch (ParseException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * 格式化日期字符串 fmt
   *
   * @param dateStr
   * @return
   */
  public static Date parseDate(String dateStr, String pattern) {
    if (StringUtils.isNotBlank(dateStr)) {
      SimpleDateFormat sdf = new SimpleDateFormat(pattern);
      try {
        return sdf.parse(dateStr);
      } catch (ParseException e) {
        logger.error("格式化数据出现异常：{}", e);
        throw new RuntimeException("格式化数据出现异常");
      }
    }
    return null;
  }

  /**
   * 获取上一年日期
   *
   * @param date
   * @return
   */
  public static Date getPreDate(Date date, Integer month, Integer day) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.YEAR, -1);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  /**
   * 获取上一年日期 精确到秒
   *
   * @param date
   * @return
   */
  public static Date getPreDate(Date date, Integer month, Integer day, Integer hour, Integer minute,
      Integer second) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.YEAR, -1);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.HOUR_OF_DAY, hour);
    cal.set(Calendar.MINUTE, minute);
    cal.set(Calendar.SECOND, second);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  /**
   * 获取当前日期
   *
   * @param date
   * @param month
   * @param day
   * @return
   */
  public static Date getCurrentDate(Date date, Integer month, Integer day) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  /**
   * 获取当前日期 精确到秒
   *
   * @param date
   * @param month
   * @param day
   * @return
   */
  public static Date getCurrentDate(Date date, Integer month, Integer day, Integer hour,
      Integer minute,
      Integer second) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.HOUR_OF_DAY, hour);
    cal.set(Calendar.MINUTE, minute);
    cal.set(Calendar.SECOND, second);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  /**
   * 获取一年有多少天(考虑闰年)
   *
   * @param date
   * @return
   */
  public static int getDaysOfYear(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int year = cal.get(Calendar.YEAR);
    int days = 365;
    if (year % 400 == 0 || (year % 4 == 0 && year % 100 != 0)) {// 判断是否闰年，闰年366天
      days = 366;
    }
    return days;
  }

  /**
   * 获取最大的时间
   *
   * @param d1
   * @param d2
   * @return
   */
  public static Date getMaxDate(Date d1, Date d2) {
    return d1.getTime() > d2.getTime() ? d1 : d2;
  }

  /**
   * 获取下一年的日期
   *
   * @param date
   * @param month
   * @param day
   * @return
   */
  public static Date getNextDate(Date date, Integer month, Integer day) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.YEAR, 1);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    return cal.getTime();
  }

  /**
   * 获取下一年的日期 精确到秒
   *
   * @param dt
   * @param month
   * @param day
   * @param hour
   * @param minute
   * @param second
   * @return
   */
  public static Date getNextDate(Date dt, Integer month, Integer day, Integer hour, Integer minute,
      Integer second) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(dt);
    cal.add(Calendar.YEAR, 1);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.HOUR_OF_DAY, hour);
    cal.set(Calendar.MINUTE, minute);
    cal.set(Calendar.SECOND, second);
    return cal.getTime();
  }

  /**
   * 格式时间
   *
   * @param dt
   * @param fmt
   * @return
   */
  public static Date parseDate2Date(Date dt, String fmt) {
    SimpleDateFormat sdf = new SimpleDateFormat(fmt);
    try {
      return sdf.parse(sdf.format(dt));
    } catch (ParseException e) {
      return null;
    }
  }

  /**
   * 时间戳变成日期时间
   *
   * @param millSec
   * @return
   */
  public static String parseLong2Date(Long millSec) {
    SimpleDateFormat sdf = new SimpleDateFormat();
    Date date = new Date(millSec);
    return sdf.format(date);
  }

  /**
   * 获取当前日期
   */
  public static String getCurrentDateStr() {
    return formatDate(new Date(), DateUtil.DATE_FMT_3);
  }

  /**
   * 获取当前时间
   */
  public static String getCurrentTimeStr() {
    return formatDate(new Date(), TIME_PATTERN);
  }

  /**
   * 获取当前日期
   */
  public static String getCurrentDateStr(String pattern) {
    return formatDate(new Date(), pattern);
  }

}
