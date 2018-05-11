/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.loader.xml;

import com.alibaba.druid.wall.WallConfig;
import com.zhongan.dmds.commons.config.ParameterMapping;
import com.zhongan.dmds.commons.util.ConfigUtil;
import com.zhongan.dmds.commons.util.DecryptUtil;
import com.zhongan.dmds.commons.util.SplitUtil;
import com.zhongan.dmds.config.Versions;
import com.zhongan.dmds.config.model.ClusterConfig;
import com.zhongan.dmds.config.model.QuarantineConfig;
import com.zhongan.dmds.config.model.SystemConfig;
import com.zhongan.dmds.config.model.UserConfig;
import com.zhongan.dmds.exception.ConfigException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class XMLServerLoader {

  private final SystemConfig system;
  private final Map<String, UserConfig> users;
  private final QuarantineConfig quarantine;
  private ClusterConfig cluster;

  public XMLServerLoader() {
    this.system = new SystemConfig();
    this.users = new HashMap<String, UserConfig>();
    this.quarantine = new QuarantineConfig();
    this.load();
  }

  public SystemConfig getSystem() {
    return system;
  }

  public Map<String, UserConfig> getUsers() {
    return (Map<String, UserConfig>) (users.isEmpty() ? Collections.emptyMap()
        : Collections.unmodifiableMap(users));
  }

  public QuarantineConfig getQuarantine() {
    return quarantine;
  }

  public ClusterConfig getCluster() {
    return cluster;
  }

  private void load() {
    InputStream dtd = null;
    InputStream xml = null;
    try {
      dtd = XMLServerLoader.class.getResourceAsStream("/server.dtd");
      xml = XMLServerLoader.class.getResourceAsStream("/server.xml");
      Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
      loadSystem(root);
      loadUsers(root);
      this.cluster = new ClusterConfig(root, system.getServerPort());
      loadQuarantine(root);
    } catch (ConfigException e) {
      throw e;
    } catch (Exception e) {
      throw new ConfigException(e);
    } finally {
      if (dtd != null) {
        try {
          dtd.close();
        } catch (IOException e) {
        }
      }
      if (xml != null) {
        try {
          xml.close();
        } catch (IOException e) {
        }
      }
    }
  }

  private void loadQuarantine(Element root)
      throws IllegalAccessException, InvocationTargetException {
    NodeList list = root.getElementsByTagName("host");
    Map<String, List<UserConfig>> whitehost = new HashMap<String, List<UserConfig>>();

    for (int i = 0, n = list.getLength(); i < n; i++) {
      Node node = list.item(i);
      if (node instanceof Element) {
        Element e = (Element) node;
        String host = e.getAttribute("host").trim();
        String userStr = e.getAttribute("user").trim();
        if (this.quarantine.existsHost(host)) {
          throw new ConfigException("host duplicated : " + host);
        }
        String[] users = userStr.split(",");
        List<UserConfig> userConfigs = new ArrayList<UserConfig>();
        for (String user : users) {
          UserConfig uc = this.users.get(user);
          if (null == uc) {
            throw new ConfigException("[user: " + user + "] doesn't exist in [host: " + host + "]");
          }
          if (uc.getSchemas() == null || uc.getSchemas().size() == 0) {
            throw new ConfigException(
                "[host: " + host + "] contains one root privileges user: " + user);
          }
          userConfigs.add(uc);
        }
        whitehost.put(host, userConfigs);
      }
    }
    quarantine.setWhitehost(whitehost);
    WallConfig wallConfig = new WallConfig();
    NodeList blacklist = root.getElementsByTagName("blacklist");
    for (int i = 0, n = blacklist.getLength(); i < n; i++) {
      Node node = blacklist.item(i);
      if (node instanceof Element) {
        Element e = (Element) node;
        String check = e.getAttribute("check");
        if (null != check) {
          quarantine.setCheck(Boolean.valueOf(check));
        }

        Map<String, Object> props = ConfigUtil.loadElements((Element) node);
        ParameterMapping.mapping(wallConfig, props);
      }
    }
    quarantine.setWallConfig(wallConfig);
    quarantine.init();

  }

  private void loadUsers(Element root) {
    NodeList list = root.getElementsByTagName("user");
    for (int i = 0, n = list.getLength(); i < n; i++) {
      Node node = list.item(i);
      if (node instanceof Element) {
        Element e = (Element) node;
        String name = e.getAttribute("name");
        UserConfig user = new UserConfig();
        Map<String, Object> props = ConfigUtil.loadElements(e);
        String password = (String) props.get("password");
        String usingDecrypt = (String) props.get("usingDecrypt");
        String passwordDecrypt = DecryptUtil.decrypt(usingDecrypt, name, password);
        user.setName(name);
        user.setPassword(passwordDecrypt);
        user.setEncryptPassword(password);

        String benchmark = (String) props.get("benchmark");
        if (null != benchmark) {
          user.setBenchmark(Integer.parseInt(benchmark));
        }

        String benchmarkSmsTel = (String) props.get("benchmarkSmsTel");
        if (null != benchmarkSmsTel) {
          user.setBenchmarkSmsTel(benchmarkSmsTel);
        }

        String readOnly = (String) props.get("readOnly");
        if (null != readOnly) {
          user.setReadOnly(Boolean.valueOf(readOnly));
        }

        String schemas = (String) props.get("schemas");
        if (schemas != null) {
          String[] strArray = SplitUtil.split(schemas, ',', true);
          user.setSchemas(new HashSet<String>(Arrays.asList(strArray)));
        }
        if (users.containsKey(name)) {
          throw new ConfigException("user " + name + " duplicated!");
        }
        users.put(name, user);
      }
    }
  }

  private void loadSystem(Element root) throws IllegalAccessException, InvocationTargetException {
    NodeList list = root.getElementsByTagName("system");
    for (int i = 0, n = list.getLength(); i < n; i++) {
      Node node = list.item(i);
      if (node instanceof Element) {
        Map<String, Object> props = ConfigUtil.loadElements((Element) node);
        ParameterMapping.mapping(system, props);
      }
    }

    if (system.getFakeMySQLVersion() != null) {
      boolean validVersion = false;
      String majorMySQLVersion = system.getFakeMySQLVersion();
      /*
       * 注意！！！ 目前MySQL官方主版本号仍然是5.x, 以后万一前面的大版本号变成2位数字， 比如 10.x...,下面获取主版本的代码要做修改
       */
      majorMySQLVersion = majorMySQLVersion.substring(0, majorMySQLVersion.indexOf(".", 2));
      for (String ver : SystemConfig.MySQLVersions) {
        // 这里只是比较mysql前面的大版本号
        if (majorMySQLVersion.equals(ver)) {
          validVersion = true;
        }
      }

      if (validVersion) {
        Versions.setServerVersion(system.getFakeMySQLVersion());
      } else {
        throw new ConfigException(
            "The specified MySQL Version (" + system.getFakeMySQLVersion() + ") is not valid.");
      }
    }
  }

}
