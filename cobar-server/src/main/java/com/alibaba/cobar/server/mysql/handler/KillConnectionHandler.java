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
 * (created at 2012-5-12)
 */
package com.alibaba.cobar.server.mysql.handler;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.alibaba.cobar.Commands;
import com.alibaba.cobar.net.packet.CommandPacket;
import com.alibaba.cobar.net.packet.ErrorPacket;
import com.alibaba.cobar.server.mysql.MySQLConnection;
import com.alibaba.cobar.server.session.ServerNIOSession;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class KillConnectionHandler implements ResponseHandler {
    private static final Logger logger = Logger.getLogger(KillConnectionHandler.class);
    private final MySQLConnection killee;
    private final ServerNIOSession session;
    private final Runnable finishHook;
    private final AtomicInteger counter;

    public KillConnectionHandler(MySQLConnection killee, ServerNIOSession session, Runnable finishHook,
                                 AtomicInteger counter) {
        this.killee = killee;
        this.session = session;
        this.finishHook = finishHook;
        this.counter = counter;
    }

    @Override
    public void connectionAcquired(MySQLConnection conn) {
        conn.setResponseHandler(this);
        CommandPacket killPacket = new CommandPacket();
        killPacket.packetId = 0;
        killPacket.command = Commands.COM_QUERY;
        killPacket.arg = new StringBuilder("KILL ").append(killee.getThreadId()).toString().getBytes();
        killPacket.write(conn);
    }

    private void finished() {
        if (counter.decrementAndGet() <= 0) {
            finishHook.run();
        }
    }

    @Override
    public void connectionError(Throwable e, MySQLConnection conn) {
        if (conn != null) {
            conn.close();
        }
        killee.close();
        finished();
    }

    @Override
    public void okPacket(byte[] ok, MySQLConnection conn) {
        conn.release();
        killee.close();
        finished();
    }

    @Override
    public void rowEnd(byte[] eof, MySQLConnection conn) {
        logger.error(new StringBuilder().append("unexpected packet for ")
                                        .append(conn)
                                        .append(" bound by ")
                                        .append(session.getSource())
                                        .append(": field's eof")
                                        .toString());
        conn.quit();
        killee.close();
        finished();
    }

    @Override
    public void errorPacket(ErrorPacket err, MySQLConnection conn) {
        String msg = null;
        try {
            msg = new String(err.message, conn.getCharset());
        } catch (UnsupportedEncodingException e) {
            msg = new String(err.message);
        }
        logger.warn("kill backend connection " + killee + " failed: " + msg);
        conn.release();
        killee.close();
        finished();
    }

    @Override
    public void fieldsEnd(byte[] header, byte[][] fields, byte[] eof, MySQLConnection conn) {
    }

    @Override
    public void rowAquired(byte[] row, MySQLConnection conn) {
    }

}
