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
/**
 * (created at 2012-4-28)
 */
package com.alibaba.cobar.server.mysql.handler;

import java.util.concurrent.Executor;

import org.apache.log4j.Logger;

import com.alibaba.cobar.net.packet.ErrorPacket;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.route.RouteResultsetNode;
import com.alibaba.cobar.server.ServerConnection;
import com.alibaba.cobar.server.mysql.MySQLConnection;
import com.alibaba.cobar.server.session.ServerNIOSession;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class CommitNodeHandler extends MultiNodeHandler {
    private static final Logger logger = Logger.getLogger(CommitNodeHandler.class);
    private OkPacket okPacket;

    public CommitNodeHandler(ServerNIOSession session) {
        super(session);
    }

    public void commit() {
        commit(null);
    }

    public void commit(OkPacket packet) {
        final int initCount = session.boundConnectionNum();
        lock.lock();
        try {
            reset(initCount);
            okPacket = packet;
        } finally {
            lock.unlock();
        }
        if (session.closed()) {
            decrementCountToZero();
            return;
        }

        // 执行
        Executor executor = session.getSource().getProcessor().getExecutor();
        int started = 0;
        for (RouteResultsetNode rrn : session.getBoundKeys()) {
            if (rrn == null) {
                try {
                    logger.error("null is contained in RoutResultsetNodes, source = " + session.getSource());
                } catch (Exception e) {
                }
                continue;
            }
            final MySQLConnection conn = session.getBoundConnection(rrn);
            if (conn != null) {
                conn.setRunning(true);
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        if (isFail.get() || session.closed()) {
                            backendConnError(conn, "cancelled by other thread");
                            return;
                        }
                        conn.setResponseHandler(CommitNodeHandler.this);
                        conn.commit();
                    }
                });
                ++started;
            }
        }

        if (started < initCount && decrementCountBy(initCount - started)) {
            /**
             * assumption: only caused by front-end connection close. <br/>
             * Otherwise, packet must be returned to front-end
             */
            session.clearConnections();
        }
    }

    @Override
    public void connectionAcquired(MySQLConnection conn) {
        logger.error("unexpected invocation: connectionAcquired from commit");
        conn.release();
    }

    @Override
    public void connectionError(Throwable e, MySQLConnection conn) {
        backendConnError(conn, "connection err for " + conn);
    }

    @Override
    public void okPacket(byte[] ok, MySQLConnection conn) {
        conn.setRunning(false);
        if (decrementCountBy(1)) {
            if (isFail.get() || session.closed()) {
                notifyError((byte) 1);
            } else {
                session.releaseConnections();
                if (okPacket == null) {
                    ServerConnection source = session.getSource();
                    source.write(ok);
                } else {
                    okPacket.write(session.getSource());
                }
            }
        }
    }

    @Override
    public void errorPacket(ErrorPacket err, MySQLConnection conn) {
        backendConnError(conn, err);
    }

    @Override
    public void rowEnd(byte[] eof, MySQLConnection conn) {
        backendConnError(conn, "Unknown response packet for back-end commit");
    }

    @Override
    public void fieldsEnd(byte[] header, byte[][] fields, byte[] eof, MySQLConnection conn) {
        logger.error(new StringBuilder().append("unexpected packet for ")
                                        .append(conn)
                                        .append(" bound by ")
                                        .append(session.getSource())
                                        .append(": field's eof")
                                        .toString());
    }

    @Override
    public void rowAquired(byte[] row, MySQLConnection conn) {
        logger.warn(new StringBuilder().append("unexpected packet for ")
                                       .append(conn)
                                       .append(" bound by ")
                                       .append(session.getSource())
                                       .append(": row data packet")
                                       .toString());
    }
}
