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
package com.alibaba.cobar.server.session;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.alibaba.cobar.CobarConfig;
import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.route.RouteResultset;
import com.alibaba.cobar.route.RouteResultsetNode;
import com.alibaba.cobar.server.ServerConnection;
import com.alibaba.cobar.server.mysql.MySQLConnection;
import com.alibaba.cobar.server.mysql.handler.CommitNodeHandler;
import com.alibaba.cobar.server.mysql.handler.KillConnectionHandler;
import com.alibaba.cobar.server.mysql.handler.MultiNodeQueryHandler;
import com.alibaba.cobar.server.mysql.handler.RollbackNodeHandler;
import com.alibaba.cobar.server.mysql.handler.RollbackReleaseHandler;
import com.alibaba.cobar.server.mysql.handler.SingleNodeHandler;
import com.alibaba.cobar.server.mysql.handler.Terminatable;
import com.alibaba.cobar.server.node.MySQLDataNode;
import com.alibaba.cobar.server.parser.ServerParse;

/**
 * @author xianmao.hexm 2012-4-12
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class ServerNIOSession {
    private static final Logger logger = Logger.getLogger(ServerNIOSession.class);
    private final ServerConnection source;
    private final ConcurrentHashMap<RouteResultsetNode, MySQLConnection> target;

    public ServerNIOSession(ServerConnection source) {
        this.source = source;
        this.target = new ConcurrentHashMap<RouteResultsetNode, MySQLConnection>(2, 1);
        this.terminating = new AtomicBoolean(false);
    }

    public ServerConnection getSource() {
        return source;
    }

    public MySQLConnection removeBoundConnection(RouteResultsetNode node) {
        return target.remove(node);
    }

    public int boundConnectionNum() {
        return target.size();
    }

    public MySQLConnection getBoundConnection(RouteResultsetNode node) {
        return target.get(node);
    }

    public Set<RouteResultsetNode> getBoundKeys() {
        return target.keySet();
    }

    public boolean closeConnection(RouteResultsetNode node) {
        MySQLConnection conn = target.remove(node);
        if (conn != null) {
            conn.close();
            return true;
        }
        return false;
    }

    public void setConnectionRunning(RouteResultsetNode[] route) {
        for (RouteResultsetNode rrn : route) {
            MySQLConnection c = target.get(rrn);
            if (c != null) {
                c.setRunning(true);
            }
        }
    }

    public void clearConnections() {
        clearConnections(true);
    }

    public void releaseConnections() {
        for (RouteResultsetNode rrn : target.keySet()) {
            MySQLConnection c = target.remove(rrn);
            if (c != null) {
                if (c.isRunning()) {
                    c.close();
                    try {
                        throw new IllegalStateException("running connection is found: " + c);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                } else if (!c.isClosedOrQuit()) {
                    if (source.isClosed()) {
                        c.quit();
                    } else {
                        c.release();
                    }
                }
            }
        }
    }

    /**
     * @return previous bound connection
     */
    public MySQLConnection boundConnection(RouteResultsetNode node, MySQLConnection connection) {
        return target.put(node, connection);
    }

    private static class Terminator {
        public Terminator nextInvocation(Terminatable term) {
            list.add(term);
            return this;
        }

        public void invoke() {
            iter = list.iterator();
            terminate();
        }

        private LinkedList<Terminatable> list = new LinkedList<Terminatable>();
        private Iterator<Terminatable> iter;

        private void terminate() {
            if (iter.hasNext()) {
                Terminatable term = iter.next();
                if (term != null) {
                    term.terminate(new Runnable() {
                        @Override
                        public void run() {
                            terminate();
                        }
                    });
                } else {
                    terminate();
                }
            }
        }
    }

    private final AtomicBoolean terminating;

    // life-cycle: each sql execution
    private volatile SingleNodeHandler singleNodeHandler;
    private volatile MultiNodeQueryHandler multiNodeHandler;
    private volatile CommitNodeHandler commitHandler;
    private volatile RollbackNodeHandler rollbackHandler;

    /**
     * {@link ServerConnection#isClosed()} must be true before invoking this
     */
    public void terminate() {
        if (!terminating.compareAndSet(false, true)) {
            return;
        }
        kill(new Runnable() {
            @Override
            public void run() {
                new Terminator().nextInvocation(singleNodeHandler)
                                .nextInvocation(multiNodeHandler)
                                .nextInvocation(commitHandler)
                                .nextInvocation(rollbackHandler)
                                .nextInvocation(new Terminatable() {
                                    @Override
                                    public void terminate(Runnable runnable) {
                                        clearConnections(false);
                                    }
                                })
                                .nextInvocation(new Terminatable() {
                                    @Override
                                    public void terminate(Runnable runnable) {
                                        terminating.set(false);
                                    }
                                })
                                .invoke();
            }
        });
    }

    private void kill(Runnable run) {
        boolean hooked = false;
        AtomicInteger count = null;
        Map<RouteResultsetNode, MySQLConnection> killees = null;
        for (RouteResultsetNode node : target.keySet()) {
            MySQLConnection c = target.get(node);
            if (c != null && c.isRunning()) {
                if (!hooked) {
                    hooked = true;
                    killees = new HashMap<RouteResultsetNode, MySQLConnection>();
                    count = new AtomicInteger(0);
                }
                killees.put(node, c);
                count.incrementAndGet();
            }
        }
        if (hooked) {
            for (Entry<RouteResultsetNode, MySQLConnection> en : killees.entrySet()) {
                KillConnectionHandler kill = new KillConnectionHandler(en.getValue(), this, run, count);
                CobarConfig conf = CobarServer.getInstance().getConfig();
                MySQLDataNode dn = conf.getDataNodes().get(en.getKey().getName());
                try {
                    dn.getConnection(kill, en.getKey());
                } catch (Exception e) {
                    logger.error("get killer connection failed for " + en.getKey(), e);
                    kill.connectionError(e, null);
                }
            }
        } else {
            run.run();
        }
    }

    private void clearConnections(boolean pessimisticRelease) {
        for (RouteResultsetNode node : target.keySet()) {
            MySQLConnection c = target.remove(node);

            if (c == null || c.isClosedOrQuit()) {
                continue;
            }

            // 如果通道正在运行中，则关闭当前通道。
            if (c.isRunning() || (pessimisticRelease && source.isClosed())) {
                c.close();
                continue;
            }

            // 非事务中的通道，直接释放资源。
            if (c.isAutocommit()) {
                c.release();
                continue;
            }

            c.setResponseHandler(new RollbackReleaseHandler());
            c.rollback();
        }
    }

    public boolean closed() {
        return source.isClosed();
    }

    public void commit() {
        final int initCount = target.size();
        if (initCount <= 0) {
            ByteBuffer buffer = source.allocate();
            buffer = source.writeToBuffer(OkPacket.OK, buffer);
            source.write(buffer);
            return;
        }
        commitHandler = new CommitNodeHandler(this);
        commitHandler.commit();
    }

    public void rollback() {
        final int initCount = target.size();
        if (initCount <= 0) {
            ByteBuffer buffer = source.allocate();
            buffer = source.writeToBuffer(OkPacket.OK, buffer);
            source.write(buffer);
            return;
        }
        rollbackHandler = new RollbackNodeHandler(this);
        rollbackHandler.rollback();
    }

    public void execute(RouteResultset rrs, int type) throws Exception {
        if (logger.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            logger.debug(s.append(source).append(rrs).toString());
        }
        RouteResultsetNode[] nodes = rrs.getNodes();
        if (nodes == null || nodes.length == 0) {
            source.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No dataNode selected");
            return;
        }

        if (nodes.length == 1) {
            singleNodeHandler = new SingleNodeHandler(nodes[0], this);
            singleNodeHandler.execute();
        } else {
            boolean autocommit = source.isAutocommit();
            if (autocommit && isModifySQL(type)) {
                autocommit = false;
            }
            multiNodeHandler = new MultiNodeQueryHandler(nodes, autocommit, this);
            multiNodeHandler.execute();
        }
    }

    private static boolean isModifySQL(int type) {
        switch (type) {
        case ServerParse.INSERT:
        case ServerParse.DELETE:
        case ServerParse.UPDATE:
        case ServerParse.REPLACE:
            return true;
        default:
            return false;
        }
    }

}
