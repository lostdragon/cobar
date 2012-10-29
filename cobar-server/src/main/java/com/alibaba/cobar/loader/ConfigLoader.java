/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cobar.loader;

import java.util.Map;
import java.util.Set;

import com.alibaba.cobar.config.util.ConfigException;
import com.alibaba.cobar.route.config.SchemaConfig;
import com.alibaba.cobar.server.config.ClusterConfig;
import com.alibaba.cobar.server.config.DataSourceConfig;
import com.alibaba.cobar.server.config.QuarantineConfig;
import com.alibaba.cobar.server.config.SystemConfig;
import com.alibaba.cobar.server.config.UserConfig;
import com.alibaba.cobar.server.node.MySQLDataNode;

/**
 * 服务器配置文件载入
 * 
 * @author xianmao.hexm
 */
public final class ConfigLoader {

    private final SystemConfig system;
    private final Map<String, UserConfig> users;
    private final Map<String, SchemaConfig> schemas;
    private final Map<String, MySQLDataNode> dataNodes;
    private final Map<String, DataSourceConfig> dataSources;
    private final ClusterConfig cluster;
    private final QuarantineConfig quarantine;

    public ConfigLoader() {
        ServerLoader serverLoader = new ServerLoader();
        SchemaLoader schemaLoader = new SchemaLoader();
        this.system = serverLoader.getSystem();
        this.users = serverLoader.getUsers();
        this.cluster = serverLoader.getCluster();
        this.quarantine = serverLoader.getQuarantine();
        this.schemas = schemaLoader.getSchemas();
        this.dataNodes = schemaLoader.getDataNodes();
        this.dataSources = schemaLoader.getDataSources();
        this.checkConfig();
    }

    private void checkConfig() throws ConfigException {
        if (users == null || users.isEmpty()) return;
        for (UserConfig uc : users.values()) {
            if (uc == null) {
                continue;
            }
            Set<String> authSchemas = uc.getSchemas();
            if (authSchemas == null) {
                continue;
            }
            for (String schema : authSchemas) {
                if (!schemas.containsKey(schema)) {
                    String errMsg = "schema " + schema + " refered by user " + uc.getName() + " is not exist!";
                    throw new ConfigException(errMsg);
                }
            }
        }

        for (SchemaConfig sc : schemas.values()) {
            if (null == sc) {
                continue;
            }
            String g = sc.getGroup();
            if (!cluster.getGroups().containsKey(g)) {
                throw new ConfigException("[group:" + g + "] refered by [schema:" + sc.getName() + "] is not exist!");
            }
        }
    }

    public SystemConfig getSystem() {
        return system;
    }

    public Map<String, UserConfig> getUsers() {
        return users;
    }

    public Map<String, SchemaConfig> getSchemas() {
        return schemas;
    }

    public Map<String, MySQLDataNode> getDataNodes() {
        return dataNodes;
    }

    public Map<String, DataSourceConfig> getDataSources() {
        return dataSources;
    }

    public ClusterConfig getCluster() {
        return cluster;
    }

    public QuarantineConfig getQuarantine() {
        return quarantine;
    }

}
