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
package com.alibaba.cobar.zookeeper;

import java.io.IOException;

import com.alibaba.cobar.net.factory.BackendConnectionFactory;
import com.alibaba.cobar.zookeeper.config.ZooKeeperConfig;

/**
 * @author xianmao.hexm
 */
public class ZooKeeperClientFactory extends BackendConnectionFactory {

    protected ZooKeeperConfig config;

    public ZooKeeperClient make() throws IOException {
        // TODO ...
        return null;
    }

    public ZooKeeperConfig getConfig() {
        return config;
    }

    public void setConfig(ZooKeeperConfig config) {
        this.config = config;
    }

}
