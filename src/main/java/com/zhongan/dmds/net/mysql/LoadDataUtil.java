/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.net.mysql;

import com.zhongan.dmds.core.BackendConnection;
import com.zhongan.dmds.core.DmdsContext;
import com.zhongan.dmds.mpp.LoadData;
import com.zhongan.dmds.net.BackendNIOConnection;
import com.zhongan.dmds.net.protocol.BinaryPacket;
import com.zhongan.dmds.route.RouteResultsetNode;

import java.io.*;
import java.util.List;

public class LoadDataUtil {

  public static void requestFileDataResponse(byte[] data, BackendConnection conn) {

    byte packId = data[3];
    BackendNIOConnection backendAIOConnection = (BackendNIOConnection) conn;
    RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
    LoadData loadData = rrn.getLoadData();
    List<String> loadDataData = loadData.getData();
    try {
      if (loadDataData != null && loadDataData.size() > 0) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0, loadDataDataSize = loadDataData.size(); i < loadDataDataSize; i++) {
          String line = loadDataData.get(i);

          String s = (i == loadDataDataSize - 1) ? line : line + loadData.getLineTerminatedBy();
          byte[] bytes = s.getBytes(loadData.getCharset());
          bos.write(bytes);

        }

        packId = writeToBackConnection(packId, new ByteArrayInputStream(bos.toByteArray()),
            backendAIOConnection);

      } else {
        // 从文件读取
        packId = writeToBackConnection(packId,
            new BufferedInputStream(new FileInputStream(loadData.getFileName())),
            backendAIOConnection);

      }
    } catch (IOException e) {

      throw new RuntimeException(e);
    } finally {
      // 结束必须发空包
      byte[] empty = new byte[]{0, 0, 0, 3};
      empty[3] = ++packId;
      backendAIOConnection.write(empty);
    }

  }

  public static byte writeToBackConnection(byte packID, InputStream inputStream,
      BackendNIOConnection backendAIOConnection) throws IOException {
    try {
      int packSize = DmdsContext.getInstance().getSystem().getProcessorBufferChunk() - 5;
      // int packSize = backendAIOConnection.getMaxPacketSize() / 32;
      // int packSize=65530;
      byte[] buffer = new byte[packSize];
      int len = -1;

      while ((len = inputStream.read(buffer)) != -1) {
        byte[] temp = null;
        if (len == packSize) {
          temp = buffer;
        } else {
          temp = new byte[len];
          System.arraycopy(buffer, 0, temp, 0, len);
        }
        BinaryPacket packet = new BinaryPacket();
        packet.packetId = ++packID;
        packet.data = temp;
        packet.write(backendAIOConnection);
      }

    } finally {
      inputStream.close();
    }

    return packID;
  }
}
