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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.alibaba.cobar.config.util.ConfigException;
import com.alibaba.cobar.config.util.ConfigUtil;
import com.alibaba.cobar.config.util.ParameterMapping;
import com.alibaba.cobar.route.config.SchemaConfig;
import com.alibaba.cobar.route.config.TableConfig;
import com.alibaba.cobar.route.config.TableRuleConfig;
import com.alibaba.cobar.server.config.DataNodeConfig;
import com.alibaba.cobar.server.config.DataSourceConfig;
import com.alibaba.cobar.server.node.MySQLDataNode;
import com.alibaba.cobar.server.node.MySQLDataSource;
import com.alibaba.cobar.util.SplitUtil;

/**
 * @author xianmao.hexm 2011-1-21 上午11:25:13
 */
public final class SchemaLoader {

    private final Map<String, TableRuleConfig> tableRules;
    private final Map<String, DataSourceConfig> dataSources;
    private final Map<String, MySQLDataNode> dataNodes;
    private final Map<String, SchemaConfig> schemas;

    public SchemaLoader() {
        this.tableRules = new RuleLoader().getTableRules();
        this.dataSources = new HashMap<String, DataSourceConfig>();
        this.dataNodes = new HashMap<String, MySQLDataNode>();
        this.schemas = new HashMap<String, SchemaConfig>();
        this.load();
    }

    public Map<String, TableRuleConfig> getTableRules() {
        return tableRules;
    }

    public Map<String, DataSourceConfig> getDataSources() {
        return dataSources;
    }

    public Map<String, MySQLDataNode> getDataNodes() {
        return dataNodes;
    }

    public Map<String, SchemaConfig> getSchemas() {
        return schemas;
    }

    private void load() {
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = RuleLoader.class.getResourceAsStream("/schema.dtd");
            xml = RuleLoader.class.getResourceAsStream("/schema.xml");
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            loadDataSources(root);
            loadDataNodes(root);
            loadSchemas(root);
        } catch (ConfigException e) {
            throw e;
        } catch (Throwable e) {
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

    private Element findPropertyByName(Element bean, String name) {
        NodeList propertyList = bean.getElementsByTagName("property");
        for (int j = 0, m = propertyList.getLength(); j < m; ++j) {
            Node node = propertyList.item(j);
            if (node instanceof Element) {
                Element p = (Element) node;
                if (name.equals(p.getAttribute("name"))) {
                    return p;
                }
            }
        }
        return null;
    }

    private void loadDataSources(Element root) {
        NodeList dataSourceList = root.getElementsByTagName("dataSource");
        for (int i = 0, n = dataSourceList.getLength(); i < n; i++) {
            Element element = (Element) dataSourceList.item(i);
            ArrayList<DataSourceConfig> dscList = new ArrayList<DataSourceConfig>();
            String dsNamePrefix = element.getAttribute("name");
            try {
                String dsType = element.getAttribute("type");
                Element locElement = findPropertyByName(element, "location");
                NodeList locationList = locElement.getElementsByTagName("location");
                int dsIndex = 0;
                for (int j = 0, m = locationList.getLength(); j < m; ++j) {
                    String locStr = ((Element) locationList.item(j)).getTextContent();
                    int colonIndex = locStr.indexOf(':');
                    int slashIndex = locStr.indexOf('/');
                    String dsHost = locStr.substring(0, colonIndex).trim();
                    int dsPort = Integer.parseInt(locStr.substring(colonIndex + 1, slashIndex).trim());
                    String[] schemas = SplitUtil.split(locStr.substring(slashIndex + 1).trim(), ',', '$', '-');
                    for (String dsSchema : schemas) {
                        DataSourceConfig channel = new DataSourceConfig();
                        ParameterMapping.mapping(channel, ConfigUtil.loadElements(element));
                        dscList.add(channel);
                        switch (dsIndex) {
                        case 0:
                            channel.setName(dsNamePrefix);
                            break;
                        case 1:
                            dscList.get(0).setName(dsNamePrefix + "[0]");
                        default:
                            channel.setName(dsNamePrefix + "[" + dsIndex + "]");
                        }
                        channel.setType(dsType);
                        channel.setDatabase(dsSchema);
                        channel.setHost(dsHost);
                        channel.setPort(dsPort);
                        ++dsIndex;
                    }
                }
            } catch (Exception e) {
                throw new ConfigException("dataSource " + dsNamePrefix + " define error", e);
            }
            for (DataSourceConfig channel : dscList) {
                if (dataSources.containsKey(channel.getName())) {
                    throw new ConfigException("dataSource name " + channel.getName() + "duplicated!");
                }
                dataSources.put(channel.getName(), channel);
            }
        }
    }

    private void loadDataNodes(Element root) {
        NodeList list = root.getElementsByTagName("dataNode");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Element element = (Element) list.item(i);
            String dnNamePrefix = element.getAttribute("name");
            List<DataNodeConfig> confList = new ArrayList<DataNodeConfig>();
            try {
                Element dsElement = findPropertyByName(element, "dataSource");
                NodeList dataSourceList = dsElement.getElementsByTagName("dataSourceRef");
                String dataSources[][] = new String[dataSourceList.getLength()][];
                for (int j = 0, m = dataSourceList.getLength(); j < m; ++j) {
                    Element ref = (Element) dataSourceList.item(j);
                    String dsString = ref.getTextContent();
                    dataSources[j] = SplitUtil.split(dsString, ',', '$', '-', '[', ']');
                }
                if (dataSources.length <= 0) {
                    throw new ConfigException("no dataSourceRef defined!");
                }
                for (String[] dss : dataSources) {
                    if (dss.length != dataSources[0].length) {
                        throw new ConfigException("dataSource number not equals!");
                    }
                }
                for (int k = 0, limit = dataSources[0].length; k < limit; ++k) {
                    StringBuilder dsString = new StringBuilder();
                    for (int dsIndex = 0; dsIndex < dataSources.length; ++dsIndex) {
                        if (dsIndex > 0) {
                            dsString.append(',');
                        }
                        dsString.append(dataSources[dsIndex][k]);
                    }
                    DataNodeConfig conf = new DataNodeConfig();
                    ParameterMapping.mapping(conf, ConfigUtil.loadElements(element));
                    confList.add(conf);
                    switch (k) {
                    case 0:
                        conf.setName((limit == 1) ? dnNamePrefix : dnNamePrefix + "[" + k + "]");
                        break;
                    default:
                        conf.setName(dnNamePrefix + "[" + k + "]");
                        break;
                    }
                    conf.setDataSource(dsString.toString());
                }
            } catch (Exception e) {
                throw new ConfigException("dataNode " + dnNamePrefix + " define error", e);
            }

            for (DataNodeConfig conf : confList) {
                MySQLDataNode dataNode = getDataNode(conf);
                if (dataNodes.containsKey(dataNode.getName())) {
                    throw new ConfigException("dataNode " + dataNode.getName() + " duplicated!");
                }
                dataNodes.put(dataNode.getName(), dataNode);
            }
        }
    }

    private void loadSchemas(Element root) {
        NodeList list = root.getElementsByTagName("schema");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Element schemaElement = (Element) list.item(i);
            String name = schemaElement.getAttribute("name");
            String dataNode = schemaElement.getAttribute("dataNode");
            // 在非空的情况下检查dataNode是否存在
            if (dataNode != null && dataNode.length() != 0) {
                checkDataNodeExists(dataNode);
            } else {
                dataNode = "";//确保非空
            }
            String group = "default";
            if (schemaElement.hasAttribute("group")) {
                group = schemaElement.getAttribute("group").trim();
            }
            Map<String, TableConfig> tables = loadTables(schemaElement);
            if (schemas.containsKey(name)) {
                throw new ConfigException("schema " + name + " duplicated!");
            }
            boolean keepSqlSchema = false;
            if (schemaElement.hasAttribute("keepSqlSchema")) {
                keepSqlSchema = Boolean.parseBoolean(schemaElement.getAttribute("keepSqlSchema").trim());
            }
            schemas.put(name, new SchemaConfig(name, dataNode, group, keepSqlSchema, tables));
        }
    }

    private Map<String, TableConfig> loadTables(Element node) {
        Map<String, TableConfig> tables = new HashMap<String, TableConfig>();
        NodeList nodeList = node.getElementsByTagName("table");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element tableElement = (Element) nodeList.item(i);
            String name = tableElement.getAttribute("name").toUpperCase();
            String dataNode = tableElement.getAttribute("dataNode");
            TableRuleConfig tableRule = null;
            if (tableElement.hasAttribute("rule")) {
                String ruleName = tableElement.getAttribute("rule");
                tableRule = tableRules.get(ruleName);
                if (tableRule == null) {
                    throw new ConfigException("rule " + ruleName + " is not found!");
                }
            }
            boolean ruleRequired = false;
            if (tableElement.hasAttribute("ruleRequired")) {
                ruleRequired = Boolean.parseBoolean(tableElement.getAttribute("ruleRequired"));
            }

            // 移除 , 分割表
            //String[] tableNames = SplitUtil.split(name, ',', true);
            //for (String tableName : tableNames) {
            TableConfig table = new TableConfig(name, dataNode, tableRule, ruleRequired);
            checkDataNodeExists(table.getDataNodes());
            if (tables.containsKey(table.getName())) {
                throw new ConfigException("table " + name + " duplicated!");
            }
            tables.put(table.getName(), table);
            //}
        }
        return tables;
    }

    private void checkDataNodeExists(String... nodes) {
        if (nodes == null || nodes.length < 1) {
            return;
        }
        for (String node : nodes) {
            if (!dataNodes.containsKey(node)) {
                throw new ConfigException("dataNode '" + node + "' is not found!");
            }
        }
    }

    private MySQLDataNode getDataNode(DataNodeConfig dnc) {
        String[] dsNames = SplitUtil.split(dnc.getDataSource(), ',');
        checkDataSourceExists(dsNames);
        MySQLDataNode node = new MySQLDataNode(dnc);
        MySQLDataSource[] dsList = new MySQLDataSource[dsNames.length];
        int size = dnc.getPoolSize();
        for (int i = 0; i < dsList.length; i++) {
            DataSourceConfig dsc = dataSources.get(dsNames[i]);
            dsList[i] = new MySQLDataSource(node, i, dsc, size);
        }
        node.setSources(dsList);
        return node;
    }

    private void checkDataSourceExists(String... nodes) {
        if (nodes == null || nodes.length < 1) {
            return;
        }
        for (String node : nodes) {
            if (!dataSources.containsKey(node)) {
                throw new ConfigException("dataSource '" + node + "' is not found!");
            }
        }
    }

}
