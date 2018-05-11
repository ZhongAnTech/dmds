/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.util;

import com.zhongan.dmds.commons.mysql.BufferUtil;
import com.zhongan.dmds.commons.mysql.MySQLMessage;
import com.zhongan.dmds.core.NIOConnection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 压缩数据包协议
 * <p>
 * http://dev.mysql.com/doc/internals/en/compressed-packet-header.html
 * <p>
 * (包头) 3 Bytes 压缩长度 1 Bytes 压缩序列号 3 Bytes 压缩前的长度
 * <p>
 * (包体) n Bytes 压缩内容 或 未压缩内容
 * <p>
 * | -------------------------------------------------------------------------------------- | |
 * comp-length | seq-id | uncomp-len | Compressed Payload | | ------------------------------------------------
 * ------------------------------------- | | 22 00 00 | 00 | 32 00 00 |
 * compress("\x2e\x00\x00\x00\x03select ...") | | --------------------------------------------------------------------------------------
 * |
 * <p>
 * Q:为什么消息体是 压缩内容 或者未压缩内容? A:这是因为mysql内部有一个约定，如果查询语句payload小于50字节时， 对内容不压缩而保持原貌的方式，而mysql此举是为了减少CPU性能开销
 *
 * 将原先的CompressUtil分成CompressUtil与DmdsCompressUtil
 */
public class CompressUtil {

  public static final int MINI_LENGTH_TO_COMPRESS = 50;
  public static final int NO_COMPRESS_PACKET_LENGTH = MINI_LENGTH_TO_COMPRESS + 4;

  /**
   * 压缩数据包
   *
   * @param input
   * @param con
   * @param compressUnfinishedDataQueue
   * @return
   */
  public static ByteBuffer compressMysqlPacket(ByteBuffer input, NIOConnection con,
      ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue) {

    byte[] byteArrayFromBuffer = getByteArrayFromBuffer(input);
    con.recycle(input);

    byteArrayFromBuffer = mergeBytes(byteArrayFromBuffer, compressUnfinishedDataQueue);
    return compressMysqlPacket(byteArrayFromBuffer, con, compressUnfinishedDataQueue);
  }

  /**
   * 压缩数据包
   *
   * @param data
   * @param con
   * @param compressUnfinishedDataQueue
   * @return
   */
  private static ByteBuffer compressMysqlPacket(byte[] data, NIOConnection con,
      ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue) {

    ByteBuffer byteBuf = con.allocate();
    byteBuf = con.checkWriteBuffer(byteBuf, data.length, false); // TODO: 数据量大的时候, 此处是是性能的堵点

    MySQLMessage msg = new MySQLMessage(data);
    while (msg.hasRemaining()) {

      // 包体的长度
      int packetLength = 0;

      // 可读的长度
      int readLength = msg.length() - msg.position();
      if (readLength > 3) {
        packetLength = msg.readUB3();
        msg.move(-3);
      }

      // 校验数据包完整性
      if (readLength < packetLength + 4) {
        byte[] packet = msg.readBytes(readLength);
        if (packet.length != 0) {
          compressUnfinishedDataQueue.add(packet); // 不完整的包
        }
      } else {

        byte[] packet = msg.readBytes(packetLength + 4);
        if (packet.length != 0) {

          if (packet.length <= NO_COMPRESS_PACKET_LENGTH) {
            BufferUtil.writeUB3(byteBuf, packet.length); // 压缩长度
            byteBuf.put(packet[3]); // 压缩序号
            BufferUtil.writeUB3(byteBuf, 0); // 压缩前的长度设置为0
            byteBuf.put(packet); // 包体

          } else {

            byte[] compress = DmdsCompressUtil.compress(packet); // 压缩

            BufferUtil.writeUB3(byteBuf, compress.length);
            byteBuf.put(packet[3]);
            BufferUtil.writeUB3(byteBuf, packet.length);
            byteBuf.put(compress);
          }
        }
      }
    }
    return byteBuf;
  }

  /**
   * 合并 解压未完成的字节
   */
  private static byte[] mergeBytes(byte[] in,
      ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue) {

    if (!decompressUnfinishedDataQueue.isEmpty()) {

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try {
        while (!decompressUnfinishedDataQueue.isEmpty()) {
          out.write(decompressUnfinishedDataQueue.poll());
        }
        out.write(in);
        in = out.toByteArray();

      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        try {
          out.close();
        } catch (IOException e) {
        }
      }
    }
    return in;
  }

  private static byte[] getByteArrayFromBuffer(ByteBuffer byteBuf) {
    byteBuf.flip();
    byte[] row = new byte[byteBuf.limit()];
    byteBuf.get(row);
    byteBuf.clear();
    return row;
  }

}
