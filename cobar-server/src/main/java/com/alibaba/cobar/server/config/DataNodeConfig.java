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
package com.alibaba.cobar.server.config;

import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.alibaba.cobar.config.util.ConfigException;
import com.alibaba.cobar.parser.ast.expression.primary.PlaceHolder;
import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.recognizer.SQLParserDelegate;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.alibaba.cobar.parser.visitor.MySQLOutputASTVisitor;

/**
 * 用于描述一个数据节点的配置
 * 
 * @author xianmao.hexm
 */
public final class DataNodeConfig {

    private static final int DEFAULT_POOL_SIZE = 128;
    private static final long DEFAULT_WAIT_TIMEOUT = 10 * 1000L;
    private static final long DEFAULT_IDLE_TIMEOUT = 10 * 60 * 1000L;
    private static final long DEFAULT_HEARTBEAT_TIMEOUT = 30 * 1000L;
    private static final int DEFAULT_HEARTBEAT_RETRY = 10;

    private String name;
    private String dataSource;
    private int poolSize = DEFAULT_POOL_SIZE;//保持后端数据通道的默认最大值
    private long waitTimeout = DEFAULT_WAIT_TIMEOUT; //取得新连接的等待超时时间
    private long idleTimeout = DEFAULT_IDLE_TIMEOUT; //连接池中连接空闲超时时间

    // heartbeat config
    private long heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT; //心跳超时时间
    private int heartbeatRetry = DEFAULT_HEARTBEAT_RETRY; //检查连接发生异常到切换，重试次数
    private String heartbeatSQL;//静态心跳语句
    private SQLStatement heartbeatAST;//动态心跳语句AST
    private Map<PlaceHolder, Object> placeHolderToStringer;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public long getWaitTimeout() {
        return waitTimeout;
    }

    public void setWaitTimeout(long waitTimeout) {
        this.waitTimeout = waitTimeout;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public long getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public void setHeartbeatTimeout(long heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public int getHeartbeatRetry() {
        return heartbeatRetry;
    }

    public void setHeartbeatRetry(int heartbeatRetry) {
        this.heartbeatRetry = heartbeatRetry;
    }

    public boolean isNeedHeartbeat() {
        return (heartbeatSQL != null || heartbeatAST != null);
    }

    public String getHeartbeat() {
        if (heartbeatSQL != null) {
            return heartbeatSQL;
        }
        if (heartbeatAST == null) {
            return null;
        }
        StringBuilder sql = new StringBuilder();
        MySQLOutputASTVisitor sqlGen = new MySQLOutputASTVisitor(sql);
        sqlGen.setPlaceHolderToString(placeHolderToStringer);
        heartbeatAST.accept(sqlGen);
        return sql.toString();
    }

    public void setHeartbeat(String heartbeat) {
        try {
            final Set<PlaceHolder> plist = new HashSet<PlaceHolder>(1, 1);
            SQLStatement ast = SQLParserDelegate.parse(heartbeat);
            ast.accept(new EmptySQLASTVisitor() {
                @Override
                public void visit(PlaceHolder node) {
                    plist.add(node);
                }
            });
            if (plist.isEmpty()) {
                heartbeatSQL = heartbeat;
                heartbeatAST = null;
                placeHolderToStringer = null;
                return;
            }
            Map<PlaceHolder, Object> phm = new HashMap<PlaceHolder, Object>(plist.size(), 1);
            for (PlaceHolder ph : plist) {
                phm.put(ph, buildToStringer(ph.getName()));
            }
            heartbeatSQL = null;
            heartbeatAST = ast;
            placeHolderToStringer = phm;
        } catch (SQLSyntaxErrorException e) {
            throw new ConfigException("heartbeat syntax err: " + heartbeat, e);
        }
    }

    private Object buildToStringer(String content) {
        final int low = Integer.parseInt(content.substring(content.indexOf('(') + 1, content.indexOf(',')).trim());
        final int high = Integer.parseInt(content.substring(content.indexOf(',') + 1, content.indexOf(')')).trim());
        return new Object() {
            private Random rnd = new Random();

            @Override
            public String toString() {
                return String.valueOf(rnd.nextInt(high - low + 1) + low);
            }
        };
    }

}
