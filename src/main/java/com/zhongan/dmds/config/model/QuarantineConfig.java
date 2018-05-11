/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.model;

import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;
import com.zhongan.dmds.config.loader.xml.XMLServerLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * 隔离区配置定义
 */
public final class QuarantineConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(QuarantineConfig.class);

  private Map<String, List<UserConfig>> whitehost;
  private List<String> blacklist;
  private boolean check = false;

  private WallConfig wallConfig = new WallConfig();

  private static WallProvider provider;

  public QuarantineConfig() {
  }

  public void init() {
    if (check) {
      provider = new MySqlWallProvider(wallConfig);
      provider.setBlackListEnable(true);
    }
  }

  public WallProvider getWallProvider() {
    return provider;
  }

  public Map<String, List<UserConfig>> getWhitehost() {
    return this.whitehost;
  }

  public void setWhitehost(Map<String, List<UserConfig>> whitehost) {
    this.whitehost = whitehost;
  }

  public boolean addWhitehost(String host, List<UserConfig> Users) {
    if (existsHost(host)) {
      return false;
    } else {
      this.whitehost.put(host, Users);
      return true;
    }
  }

  public List<String> getBlacklist() {
    return this.blacklist;
  }

  public void setBlacklist(List<String> blacklist) {
    this.blacklist = blacklist;
  }

  public WallProvider getProvider() {
    return provider;
  }

  public boolean existsHost(String host) {
    return this.whitehost == null ? false : whitehost.get(host) != null;
  }

  public boolean canConnect(String host, String user, Map<String, UserConfig> users) {
    if (whitehost == null || whitehost.size() == 0) {
      return users.containsKey(user);
    } else {
      List<UserConfig> list = whitehost.get(host);
      if (list == null) {
        return false;
      }
      for (UserConfig userConfig : list) {
        if (userConfig.getName().equals(user)) {
          return true;
        }
      }
    }
    return false;
  }

  public static void setProvider(WallProvider provider) {
    QuarantineConfig.provider = provider;
  }

  public void setWallConfig(WallConfig wallConfig) {
    this.wallConfig = wallConfig;

  }

  public boolean isCheck() {
    return this.check;
  }

  public void setCheck(boolean check) {
    this.check = check;
  }

  public WallConfig getWallConfig() {
    return this.wallConfig;
  }

  public synchronized static void updateToFile(String host, List<UserConfig> userConfigs)
      throws Exception {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("set white host:" + host + "user:" + userConfigs);
    }
    String filename =
        SystemConfig.getHomePath() + File.separator + "conf" + File.separator + "server.xml";

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(false);
    factory.setValidating(false);
    DocumentBuilder builder = factory.newDocumentBuilder();
    builder.setEntityResolver(new IgnoreDTDEntityResolver());
    Document xmldoc = builder.parse(filename);
    Element whitehost = (Element) xmldoc.getElementsByTagName("whitehost").item(0);
    Element quarantine = (Element) xmldoc.getElementsByTagName("quarantine").item(0);

    if (quarantine == null) {
      quarantine = xmldoc.createElement("quarantine");
      Element root = xmldoc.getDocumentElement();
      root.appendChild(quarantine);
      if (whitehost == null) {
        whitehost = xmldoc.createElement("whitehost");
        quarantine.appendChild(whitehost);
      }
    }

    for (UserConfig userConfig : userConfigs) {
      String user = userConfig.getName();
      Element hostEle = xmldoc.createElement("host");
      hostEle.setAttribute("host", host);
      hostEle.setAttribute("user", user);

      whitehost.appendChild(hostEle);
    }

    TransformerFactory factory2 = TransformerFactory.newInstance();
    Transformer former = factory2.newTransformer();
    String systemId = xmldoc.getDoctype().getSystemId();
    if (systemId != null) {
      former.setOutputProperty(javax.xml.transform.OutputKeys.DOCTYPE_SYSTEM, systemId);
    }
    former.transform(new DOMSource(xmldoc), new StreamResult(new File(filename)));

  }

  static class IgnoreDTDEntityResolver implements EntityResolver {

    public InputSource resolveEntity(java.lang.String publicId, java.lang.String systemId)
        throws SAXException, java.io.IOException {
      if (systemId.contains("server.dtd")) {
        InputStream dtd = XMLServerLoader.class.getResourceAsStream("/server.dtd");
        InputSource is = new InputSource(dtd);
        return is;
      } else {
        return null;
      }
    }
  }
}