/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql;

import com.zhongan.dmds.commons.mysql.BindValue;
import com.zhongan.dmds.commons.mysql.MySQLMessage;
import com.zhongan.dmds.config.Fields;

import java.io.UnsupportedEncodingException;

public class BindValueUtil {

  public static final void read(MySQLMessage mm, BindValue bv, String charset)
      throws UnsupportedEncodingException {
    switch (bv.type & 0xff) {
      case Fields.FIELD_TYPE_BIT:
        bv.value = mm.readBytesWithLength();
        break;
      case Fields.FIELD_TYPE_TINY:
        bv.byteBinding = mm.read();
        break;
      case Fields.FIELD_TYPE_SHORT:
        bv.shortBinding = (short) mm.readUB2();
        break;
      case Fields.FIELD_TYPE_LONG:
        bv.intBinding = mm.readInt();
        break;
      case Fields.FIELD_TYPE_LONGLONG:
        bv.longBinding = mm.readLong();
        break;
      case Fields.FIELD_TYPE_FLOAT:
        bv.floatBinding = mm.readFloat();
        break;
      case Fields.FIELD_TYPE_DOUBLE:
        bv.doubleBinding = mm.readDouble();
        break;
      case Fields.FIELD_TYPE_TIME:
        bv.value = mm.readTime();
        break;
      case Fields.FIELD_TYPE_DATE:
      case Fields.FIELD_TYPE_DATETIME:
      case Fields.FIELD_TYPE_TIMESTAMP:
        bv.value = mm.readDate();
        break;
      case Fields.FIELD_TYPE_VAR_STRING:
      case Fields.FIELD_TYPE_STRING:
      case Fields.FIELD_TYPE_VARCHAR:
        bv.value = mm.readStringWithLength(charset);
        if (bv.value == null) {
          bv.isNull = true;
        }
        break;
      case Fields.FIELD_TYPE_DECIMAL:
      case Fields.FIELD_TYPE_NEW_DECIMAL:
        bv.value = mm.readBigDecimal();
        if (bv.value == null) {
          bv.isNull = true;
        }
        break;
      default:
        throw new IllegalArgumentException("bindValue error,unsupported type:" + bv.type);
    }
    bv.isSet = true;
  }

}