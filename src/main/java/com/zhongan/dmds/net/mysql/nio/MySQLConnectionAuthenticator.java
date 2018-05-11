/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql.nio;

import com.zhongan.dmds.commons.util.SecurityUtil;
import com.zhongan.dmds.config.Capabilities;
import com.zhongan.dmds.config.util.CharsetUtil;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.core.NIOHandler;
import com.zhongan.dmds.core.ResponseHandler;
import com.zhongan.dmds.exception.ConnectionException;
import com.zhongan.dmds.net.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL 验证处理器
 */
public class MySQLConnectionAuthenticator implements NIOHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLConnectionAuthenticator.class);
  private final MySQLConnection source;
  private final ResponseHandler listener;

  public MySQLConnectionAuthenticator(MySQLConnection source, ResponseHandler listener) {
    this.source = source;
    this.listener = listener;
  }

  public void connectionError(MySQLConnection source, Throwable e) {
    listener.connectionError(e, source);
  }

  /**
   * MySQL 4.1版本之前是MySQL323加密,MySQL 4.1和之后的版本都是MySQLSHA1加密，在MySQL5.5的版本之后可以客户端插件式加密
   *
   * @see @http://dev.mysql.com/doc/internals/en/determining-authentication-method.html
   */
  @Override
  public void handle(byte[] data) {
    try {
      switch (data[4]) {
        // 如果是OkPacket，检查是否认证成功
        case OkPacket.FIELD_COUNT:
          HandshakePacket packet = source.getHandshake();
          if (packet == null) {
            // 如果为null，证明链接第一次建立，处理
            processHandShakePacket(data);
            // 发送认证数据包
            source.authenticate();
            break;
          }
          // 如果packet不为null，处理认证结果
          // 首先将连接设为已验证并将handler改为MySQLConnectionHandler
          source.setHandler(new MySQLConnectionHandler(source));
          source.setAuthenticated(true);
          // 判断是否用了压缩协议
          boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS
              & packet.serverCapabilities);
          boolean usingCompress = DmdsContext.getInstance().getSystem().getUseCompression() == 1;
          if (clientCompress && usingCompress) {
            source.setSupportCompress(true);
          }
          // 设置ResponseHandler
          if (listener != null) {
            listener.connectionAcquired(source);
          }
          break;
        // 如果为ErrorPacket，则认证失败
        case ErrorPacket.FIELD_COUNT:
          ErrorPacket err = new ErrorPacket();
          err.read(data);
          String errMsg = new String(err.message);
          LOGGER.warn("can't connect to mysql server ,errmsg:" + errMsg + " " + source);
          throw new ConnectionException(err.errno, errMsg);
          // 如果是EOFPacket，则为MySQL 4.1版本，是MySQL323加密
        case EOFPacket.FIELD_COUNT:
          auth323(data[3]);
          break;
        default:
          packet = source.getHandshake();
          if (packet == null) {
            processHandShakePacket(data);
            // 发送认证数据包
            source.authenticate();
            break;
          } else {
            throw new RuntimeException("Unknown Packet!");
          }

      }

    } catch (RuntimeException e) {
      if (listener != null) {
        listener.connectionError(e, source);
        return;
      }
      throw e;
    }
  }

  private void processHandShakePacket(byte[] data) {
    // 设置握手数据包
    HandshakePacket packet = new HandshakePacket();
    packet.read(data);
    source.setHandshake(packet);
    source.setThreadId(packet.threadId);

    // 设置字符集编码
    int charsetIndex = (packet.serverCharsetIndex & 0xff);
    String charset = CharsetUtil.getCharset(charsetIndex);
    if (charset != null) {
      source.setCharset(charset);
    } else {
      throw new RuntimeException("Unknown charsetIndex:" + charsetIndex);
    }
  }

  private void auth323(byte packetId) {
    // 发送323响应认证数据包
    Reply323Packet r323 = new Reply323Packet();
    r323.packetId = ++packetId;
    String pass = source.getPassword();
    if (pass != null && pass.length() > 0) {
      byte[] seed = source.getHandshake().seed;
      r323.seed = SecurityUtil.scramble323(pass, new String(seed)).getBytes();
    }
    r323.write(source);
  }

}