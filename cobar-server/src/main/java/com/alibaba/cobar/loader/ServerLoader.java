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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.alibaba.cobar.config.util.ConfigException;
import com.alibaba.cobar.config.util.ConfigUtil;
import com.alibaba.cobar.config.util.ParameterMapping;
import com.alibaba.cobar.server.config.ClusterConfig;
import com.alibaba.cobar.server.config.CobarNodeConfig;
import com.alibaba.cobar.server.config.QuarantineConfig;
import com.alibaba.cobar.server.config.SystemConfig;
import com.alibaba.cobar.server.config.UserConfig;
import com.alibaba.cobar.server.node.CobarNode;
import com.alibaba.cobar.util.SplitUtil;

/**
 * 服务器配置文件载入
 * 
 * @author xianmao.hexm 2011-1-10 下午02:40:53
 */
public final class ServerLoader {

    private final SystemConfig system;
    private final Map<String, UserConfig> users;
    private final ClusterConfig cluster;
    private final QuarantineConfig quarantine;

    public ServerLoader() {
        this.system = new SystemConfig();
        this.users = new HashMap<String, UserConfig>();
        this.cluster = new ClusterConfig();
        this.quarantine = new QuarantineConfig();
        this.load();
    }

    public SystemConfig getSystem() {
        return system;
    }

    public Map<String, UserConfig> getUsers() {
        return users;
    }

    public ClusterConfig getCluster() {
        return cluster;
    }

    public QuarantineConfig getQuarantine() {
        return quarantine;
    }

    private void load() {
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = ConfigLoader.class.getResourceAsStream("/server.dtd");
            xml = ConfigLoader.class.getResourceAsStream("/server.xml");
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            loadSystem(root);
            loadUsers(root);
            loadCluster(root);
            loadQuarantine(root);
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

    private void loadSystem(Element root) throws IllegalAccessException, InvocationTargetException {
        NodeList list = root.getElementsByTagName("system");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Map<String, Object> props = ConfigUtil.loadElements((Element) node);
                ParameterMapping.mapping(system, props);
            }
        }
    }

    private void loadUsers(Element root) {
        NodeList list = root.getElementsByTagName("user");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getAttribute("name");
                UserConfig user = new UserConfig();
                user.setName(name);
                Map<String, Object> props = ConfigUtil.loadElements(e);
                user.setPassword((String) props.get("password"));
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

    private void loadCluster(Element root) {
        loadNode(root);
        loadGroup(root);
    }

    private void loadNode(Element root) {
        NodeList list = root.getElementsByTagName("node");
        Set<String> hostSet = new HashSet<String>();
        int port = system.getServerPort();
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element element = (Element) node;
                String name = element.getAttribute("name").trim();
                if (cluster.getNodes().containsKey(name)) {
                    throw new ConfigException("node name duplicated :" + name);
                }

                Map<String, Object> props = ConfigUtil.loadElements(element);
                String host = (String) props.get("host");
                if (null == host || "".equals(host)) {
                    throw new ConfigException("host empty in node: " + name);
                }
                if (hostSet.contains(host)) {
                    throw new ConfigException("node host duplicated :" + host);
                }

                String wei = (String) props.get("weight");
                if (null == wei || "".equals(wei)) {
                    throw new ConfigException("weight should not be null in host:" + host);
                }
                int weight = Integer.valueOf(wei);
                if (weight <= 0) {
                    throw new ConfigException("weight should be > 0 in host:" + host + " weight:" + weight);
                }

                CobarNodeConfig conf = new CobarNodeConfig(name, host, port, weight);
                cluster.getNodes().put(name, new CobarNode(conf));
                hostSet.add(host);
            }
        }
    }

    private void loadGroup(Element root) {
        NodeList list = root.getElementsByTagName("group");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String groupName = e.getAttribute("name").trim();
                if (cluster.getGroups().containsKey(groupName)) {
                    throw new ConfigException("group duplicated : " + groupName);
                }

                Map<String, Object> props = ConfigUtil.loadElements(e);
                String value = (String) props.get("nodeList");
                if (null == value || "".equals(value)) {
                    throw new ConfigException("group should contain 'nodeList'");
                }

                String[] sList = SplitUtil.split(value, ',', true);

                if (null == sList || sList.length == 0) {
                    throw new ConfigException("group should contain 'nodeList'");
                }

                for (String s : sList) {
                    if (!cluster.getNodes().containsKey(s)) {
                        throw new ConfigException("[ node :" + s + "] in [ group:" + groupName + "] doesn't exist!");
                    }
                }
                List<String> nodeList = Arrays.asList(sList);
                cluster.getGroups().put(groupName, nodeList);
            }
        }
        if (!cluster.getGroups().containsKey("default")) {
            List<String> nodeList = new ArrayList<String>(cluster.getNodes().keySet());
            cluster.getGroups().put("default", nodeList);
        }
    }

    private void loadQuarantine(Element root) {
        NodeList list = root.getElementsByTagName("host");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String host = e.getAttribute("name").trim();
                if (quarantine.getHosts().containsKey(host)) {
                    throw new ConfigException("host duplicated : " + host);
                }

                Map<String, Object> props = ConfigUtil.loadElements(e);
                String[] users = SplitUtil.split((String) props.get("user"), ',', true);
                HashSet<String> set = new HashSet<String>();
                if (null != users) {
                    for (String user : users) {
                        UserConfig uc = this.users.get(user);
                        if (null == uc) {
                            throw new ConfigException("[user: " + user + "] doesn't exist in [host: " + host + "]");
                        }

                        if (null == uc.getSchemas() || uc.getSchemas().size() == 0) {
                            throw new ConfigException("[host: " + host + "] contains one root privileges user: " + user);
                        }
                        if (set.contains(user)) {
                            throw new ConfigException("[host: " + host + "] contains duplicate user: " + user);
                        } else {
                            set.add(user);
                        }
                    }
                }
                quarantine.getHosts().put(host, set);
            }
        }
    }
}
