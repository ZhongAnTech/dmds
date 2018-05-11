/*
 * Copyright (C) 2016-2020 zhongan.com
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.zhongan.dmds.config.loader;

import com.zhongan.dmds.config.model.DataHostConfig;
import com.zhongan.dmds.config.model.DataNodeConfig;
import com.zhongan.dmds.config.model.SchemaConfig;
import com.zhongan.dmds.config.model.rule.TableRuleConfig;

import java.util.Map;

public interface SchemaLoader {

  Map<String, TableRuleConfig> getTableRules();

  Map<String, DataHostConfig> getDataHosts();

  Map<String, DataNodeConfig> getDataNodes();

  Map<String, SchemaConfig> getSchemas();

}