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
package com.alibaba.cobar.server.node;

/**
 * @author xianmao.hexm 2011-5-6 下午03:10:16
 */
public final class MySQLChannelFactory implements ChannelFactory {

    @Override
    public Channel make(MySQLDataSource dataSource) {
        return new MySQLChannel(dataSource);
    }

}
