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
 * (created at 2012-4-19)
 */
package com.alibaba.cobar.server.mysql.handler;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.cobar.CobarConfig;
import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.net.packet.ErrorPacket;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.route.RouteResultsetNode;
import com.alibaba.cobar.server.ServerConnection;
import com.alibaba.cobar.server.mysql.MySQLConnection;
import com.alibaba.cobar.server.mysql.MySQLConnection.StatusSync;
import com.alibaba.cobar.server.node.MySQLDataNode;
import com.alibaba.cobar.server.session.ServerNIOSession;
import com.alibaba.cobar.util.StringUtil;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class SingleNodeHandler implements ResponseHandler, Terminatable {
    private final RouteResultsetNode route;
    private final ServerNIOSession session;
    private byte packetId;
    private volatile ByteBuffer buffer;
    private ReentrantLock lock = new ReentrantLock();
    private boolean isRunning;
    private Runnable terminateCallBack;

    public SingleNodeHandler(RouteResultsetNode route, ServerNIOSession session) {
        if (route == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        if (session == null) {
            throw new IllegalArgumentException("session is null!");
        }
        this.session = session;
        this.route = route;
    }

    @Override
    public void terminate(Runnable callback) {
        boolean zeroReached = false;
        lock.lock();
        try {
            if (isRunning) {
                terminateCallBack = callback;
            } else {
                zeroReached = true;
            }
        } finally {
            lock.unlock();
        }
        if (zeroReached) {
            callback.run();
        }
    }

    private void endRunning() {
        Runnable callback = null;
        lock.lock();
        try {
            if (isRunning) {
                isRunning = false;
                callback = terminateCallBack;
                terminateCallBack = null;
            }
        } finally {
            lock.unlock();
        }
        if (callback != null) {
            callback.run();
        }
    }

    public void execute() throws Exception {
        lock.lock();
        try {
            this.isRunning = true;
            this.packetId = 0;
            this.buffer = session.getSource().allocate();
        } finally {
            lock.unlock();
        }
        final MySQLConnection conn = session.getBoundConnection(route);
        if (conn == null) {
            CobarConfig conf = CobarServer.getInstance().getConfig();
            MySQLDataNode dn = conf.getDataNodes().get(route.getName());
            dn.getConnection(this, null);
        } else {
            conn.setRunning(true);
            session.getSource().getProcessor().getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    _execute(conn);
                }
            });
        }
    }

    @Override
    public void connectionAcquired(final MySQLConnection conn) {
        conn.setRunning(true);
        session.boundConnection(route, conn);
        session.getSource().getProcessor().getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                _execute(conn);
            }
        });
    }

    private void _execute(MySQLConnection conn) {
        if (session.closed()) {
            conn.setRunning(false);
            endRunning();
            session.clearConnections();
            return;
        }
        conn.setResponseHandler(this);
        try {
            conn.execute(route, session.getSource(), session.getSource().isAutocommit());
        } catch (UnsupportedEncodingException e1) {
            executeException(conn);
            return;
        }
    }

    private void executeException(MySQLConnection c) {
        c.setRunning(false);
        endRunning();
        session.clearConnections();
        ErrorPacket err = new ErrorPacket();
        err.packetId = ++packetId;
        err.errno = ErrorCode.ER_YES;
        err.message = StringUtil.encode("unknown backend charset: " + c.getCharset(), session.getSource().getCharset());
        ServerConnection source = session.getSource();
        source.write(err.write(buffer, source));
    }

    @Override
    public void connectionError(Throwable e, MySQLConnection conn) {
        if (!session.closeConnection(route)) {
            conn.close();
        }
        endRunning();
        ErrorPacket err = new ErrorPacket();
        err.packetId = ++packetId;
        err.errno = ErrorCode.ER_YES;
        err.message = StringUtil.encode(e.getMessage(), session.getSource().getCharset());
        ServerConnection source = session.getSource();
        source.write(err.write(buffer, source));
    }

    @Override
    public void errorPacket(ErrorPacket err, MySQLConnection conn) {
        conn.setRunning(false);
        if (conn.isAutocommit()) {
            session.clearConnections();
        }
        endRunning();
        ServerConnection source = session.getSource();
        source.write(err.write(buffer, source));
    }

    @Override
    public void okPacket(byte[] data, MySQLConnection conn) {
        StatusSync sync = conn.getStatusSync();
        if (!sync.isExecuted()) {
            if (sync.isSync()) {
                sync.update();
                try {
                    sync.execute();
                } catch (UnsupportedEncodingException e) {
                    executeException(conn);
                }
            } else {
                sync.update();
                sync.sync();
            }
        } else {
            conn.setRunning(false);
            ServerConnection source = session.getSource();
            if (source.isAutocommit()) {
                session.clearConnections();
            }
            endRunning();
            OkPacket ok = new OkPacket();
            ok.read(data);
            source.setLastInsertId(ok.insertId);
            buffer = source.writeToBuffer(data, buffer);
            source.write(buffer);
        }
    }

    @Override
    public void rowEnd(byte[] eof, MySQLConnection conn) {
        ServerConnection source = session.getSource();
        conn.setRunning(false);
        conn.recordSql(source.getHost(), source.getSchema(), route.getStatement());
        if (source.isAutocommit()) {
            session.clearConnections();
        }
        endRunning();
        buffer = source.writeToBuffer(eof, buffer);
        source.write(buffer);
    }

    @Override
    public void fieldsEnd(byte[] header, byte[][] fields, byte[] eof, MySQLConnection conn) {
        ServerConnection source = session.getSource();
        buffer = session.getSource().allocate();
        ++packetId;
        buffer = source.writeToBuffer(header, buffer);
        for (int i = 0, len = fields.length; i < len; ++i) {
            ++packetId;
            buffer = source.writeToBuffer(fields[i], buffer);
        }
        ++packetId;
        buffer = source.writeToBuffer(eof, buffer);
        source.write(buffer);
    }

    @Override
    public void rowAquired(byte[] row, MySQLConnection conn) {
        ++packetId;
        buffer = session.getSource().writeToBuffer(row, buffer);
    }

}
