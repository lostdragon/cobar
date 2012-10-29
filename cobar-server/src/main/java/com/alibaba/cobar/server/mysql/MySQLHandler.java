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
package com.alibaba.cobar.server.mysql;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.cobar.net.handler.NIOHandler;
import com.alibaba.cobar.net.packet.EOFPacket;
import com.alibaba.cobar.net.packet.ErrorPacket;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.net.util.ByteUtil;
import com.alibaba.cobar.server.mysql.handler.ResponseHandler;

/**
 * life cycle: from connection establish to close <br/>
 * 
 * @author xianmao.hexm 2012-4-12
 */
public class MySQLHandler implements NIOHandler {
    //    private static final Logger logger = Logger.getLogger(MySQLHandler.class);

    private final MySQLConnection source;
    private final BlockingQueue<byte[]> dataQueue;
    private final AtomicBoolean handleStatus;
    /** always be negative number */
    private static final int RESULT_STATUS_INIT = -1;
    /** always equals 0 */
    private static final int RESULT_STATUS_HEADER = 0;
    /** always be negative number */
    private static final int RESULT_STATUS_FIRST_EOF = -2;
    private final AtomicInteger resultStatus;
    private volatile byte[] header;
    private volatile byte[][] fields;
    private volatile Throwable connError;

    public MySQLHandler(MySQLConnection source) {
        this.source = source;
        //QS_TODO jsr166y.LinkedTransferQueue
        this.dataQueue = new LinkedBlockingQueue<byte[]>();
        this.handleStatus = new AtomicBoolean(false);
        this.resultStatus = new AtomicInteger(RESULT_STATUS_INIT);
    }

    private void reset() {
        dataQueue.clear();
        handleStatus.set(false);
        header = null;
        fields = null;
        resultStatus.set(RESULT_STATUS_INIT);
        connError = null;
    }

    /**
     * life cycle: one SQL execution
     */
    private volatile ResponseHandler responseHandler;

    public void setResponseHandler(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
    }

    public MySQLConnection getSource() {
        return source;
    }

    private boolean offerData(byte[] data) {
        //QS_TODO data flow control
        return dataQueue.offer(data);
    }

    @Override
    public void handle(byte[] data) {
        if (offerData(data)) {
            handleQueue();
        } else {
            throw new RuntimeException("Add data to queue failure!");
        }
    }

    public void connectionError(Throwable e) {
        connError = e;
        handleQueue();
    }

    /**
     * 处理队列
     */
    private void handleQueue() {
        if (!handleStatus.compareAndSet(false, true)) {
            return;
        }
        source.getProcessor().getExecutor().execute(new Runnable() {

            @Override
            public void run() {
                try {
                    byte[] data = null;
                    while ((data = dataQueue.poll()) != null) {
                        handleData(data);
                    }
                    Throwable err = connError;
                    if (err != null) {
                        reset();
                        responseHandler.connectionError(err, source);
                    }
                } finally {
                    handleStatus.set(false);
                    if (dataQueue.size() > 0) {
                        handleQueue();
                    }
                }
            }
        });
    }

    private void handleData(byte[] data) {
        if (resultStatus.get() == RESULT_STATUS_INIT) {
            switch (data[4]) {
            case OkPacket.FIELD_COUNT://end
                reset();
                byte[] ok = data;
                responseHandler.okPacket(ok, source);
                break;
            case ErrorPacket.FIELD_COUNT://end
                reset();
                ErrorPacket err = new ErrorPacket();
                err.read(data);
                responseHandler.errorPacket(err, source);
                break;
            default://rs_header
                resultStatus.set(RESULT_STATUS_HEADER);
                header = data;
                fields = new byte[(int) ByteUtil.readLength(data, 4)][];
            }
        } else {
            switch (data[4]) {
            case ErrorPacket.FIELD_COUNT://end    
                reset();
                ErrorPacket err = new ErrorPacket();
                err.read(data);
                responseHandler.errorPacket(err, source);
                break;
            case EOFPacket.FIELD_COUNT:
                byte[] eof = data;
                if (resultStatus.get() == RESULT_STATUS_FIRST_EOF) {//end
                    reset();
                    responseHandler.rowEnd(eof, source);
                } else {
                    resultStatus.set(RESULT_STATUS_FIRST_EOF);
                    responseHandler.fieldsEnd(header, fields, eof, source);
                    fields = null;
                }
                break;
            default:
                if (resultStatus.get() >= RESULT_STATUS_HEADER) {//fields
                    fields[resultStatus.getAndIncrement()] = data;
                } else {//rows
                    byte[] row = data;
                    responseHandler.rowAquired(row, source);
                }
            }
        }
    }

}
