/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio;

import com.zhongan.dmds.core.BackendAsyncHandler;
import com.zhongan.dmds.core.ResponseHandler;
import com.zhongan.dmds.net.mysql.ByteUtil;
import com.zhongan.dmds.net.mysql.nio.handler.LoadDataResponseHandler;
import com.zhongan.dmds.net.protocol.EOFPacket;
import com.zhongan.dmds.net.protocol.ErrorPacket;
import com.zhongan.dmds.net.protocol.OkPacket;
import com.zhongan.dmds.net.protocol.RequestFilePacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * life cycle: from connection establish to close <br/>
 */
public class MySQLConnectionHandler extends BackendAsyncHandler {

  private static final Logger logger = LoggerFactory.getLogger(MySQLConnectionHandler.class);
  private static final int RESULT_STATUS_INIT = 0;
  private static final int RESULT_STATUS_HEADER = 1;
  private static final int RESULT_STATUS_FIELD_EOF = 2;

  private final MySQLConnection source;
  private volatile int resultStatus;
  private volatile byte[] header;
  private volatile List<byte[]> fields;

  /**
   * life cycle: one SQL execution
   */
  private volatile ResponseHandler responseHandler;

  public MySQLConnectionHandler(MySQLConnection source) {
    this.source = source;
    this.resultStatus = RESULT_STATUS_INIT;
  }

  public void connectionError(Throwable e) {
    if (responseHandler != null) {
      responseHandler.connectionError(e, source);
    }

  }

  public MySQLConnection getSource() {
    return source;
  }

  @Override
  public void handle(byte[] data) {
    offerData(data, source.getProcessor().getExecutor());
  }

  @Override
  protected void offerDataError() {
    resultStatus = RESULT_STATUS_INIT;
    throw new RuntimeException("offer data error!");
  }

  /**
   * 可以分为三个阶段： （第一阶段）客户端发送查询请求包COM_QUERY （command query packet），如果有结果集返回，且结果集不为空，则返回FieldCount（列数量）包；如果结果集为空，则返回OKPacket；如果命令有错，则返回ERRPacket；如果是Load
   * file data命令，则返回LOCAL_INFILE_Request。 （第二阶段）如果有结果集返回，则先返回列集合，所有列返回完了之后，会返回EOFPacket；如果过程中出现错误，则返回错误包。
   * （第三阶段）之后返回行记录，返回全部行记录之后，返回EOFPacket。如果有错误，回错误包
   */
  @Override
  protected void handleData(byte[] data) {

    switch (resultStatus) {
      // 第一阶段
      case RESULT_STATUS_INIT:
        switch (data[4]) {
          // 返回OKPacket
          case OkPacket.FIELD_COUNT:
            handleOkPacket(data);
            break;
          // 返回错误包
          case ErrorPacket.FIELD_COUNT:
            handleErrorPacket(data);
            break;
          // 返回Load Data进一步操作
          case RequestFilePacket.FIELD_COUNT:
            handleRequestPacket(data);
            break;
          // 返回结果集列数量
          default:
            // 记录列数量并进入第二阶段
            resultStatus = RESULT_STATUS_HEADER;
            header = data;
            fields = new ArrayList<byte[]>((int) ByteUtil.readLength(data, 4));
        }
        break;
      // 第二阶段
      case RESULT_STATUS_HEADER:
        switch (data[4]) {
          // 返回错误包
          case ErrorPacket.FIELD_COUNT:
            resultStatus = RESULT_STATUS_INIT;
            handleErrorPacket(data);
            break;
          // 返回EOF，证明列集合返回完毕，进入第三阶段
          case EOFPacket.FIELD_COUNT:
            resultStatus = RESULT_STATUS_FIELD_EOF;
            handleFieldEofPacket(data);
            break;
          // 返回的是列集合，记录
          default:
            fields.add(data);
        }
        break;
      // 第三阶段
      case RESULT_STATUS_FIELD_EOF:
        switch (data[4]) {
          // 返回错误包
          case ErrorPacket.FIELD_COUNT:
            resultStatus = RESULT_STATUS_INIT;
            handleErrorPacket(data);
            break;
          // 返回EOF，证明结果集返回完毕，回到第一阶段等待下一个请求的响应
          case EOFPacket.FIELD_COUNT:
            resultStatus = RESULT_STATUS_INIT;
            handleRowEofPacket(data);
            break;
          // 返回结果集包
          default:
            handleRowPacket(data);
        }
        break;
      default:
        throw new RuntimeException("unknown status!");
    }
  }

  public void setResponseHandler(ResponseHandler responseHandler) {
    // logger.info("set response handler "+responseHandler);
    // if (this.responseHandler != null && responseHandler != null) {
    // throw new RuntimeException("reset again!");
    // }
    this.responseHandler = responseHandler;
  }

  /**
   * OK数据包处理
   */
  private void handleOkPacket(byte[] data) {
    ResponseHandler respHand = responseHandler;
    if (respHand != null) {
      respHand.okResponse(data, source);
    }
  }

  /**
   * ERROR数据包处理
   */
  private void handleErrorPacket(byte[] data) {
    ResponseHandler respHand = responseHandler;
    if (respHand != null) {
      respHand.errorResponse(data, source);
    } else {
      closeNoHandler();
    }
  }

  /**
   * load data file 请求文件数据包处理
   */
  private void handleRequestPacket(byte[] data) {
    ResponseHandler respHand = responseHandler;
    if (respHand != null && respHand instanceof LoadDataResponseHandler) {
      ((LoadDataResponseHandler) respHand).requestDataResponse(data, source);
    } else {
      closeNoHandler();
    }
  }

  /**
   * 字段数据包结束处理
   */
  private void handleFieldEofPacket(byte[] data) {
    ResponseHandler respHand = responseHandler;
    if (respHand != null) {
      respHand.fieldEofResponse(header, fields, data, source);
    } else {
      closeNoHandler();
    }
  }

  /**
   * 行数据包处理
   */
  private void handleRowPacket(byte[] data) {
    ResponseHandler respHand = responseHandler;
    if (respHand != null) {
      respHand.rowResponse(data, source);
    } else {
      closeNoHandler();

    }
  }

  private void closeNoHandler() {
    if (!source.isClosedOrQuit()) {
      source.close("no handler");
      logger.warn("no handler bind in this con " + this + " client:" + source);
    }
  }

  /**
   * 行数据包结束处理
   */
  private void handleRowEofPacket(byte[] data) {
    if (responseHandler != null) {
      responseHandler.rowEofResponse(data, source);
    } else {
      closeNoHandler();
    }
  }

}