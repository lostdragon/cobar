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
package com.alibaba.cobar.server.heartbeat;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.net.handler.NIOHandler;
import com.alibaba.cobar.net.packet.EOFPacket;
import com.alibaba.cobar.net.packet.ErrorPacket;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.server.HeartbeatException;

/**
 * @author xianmao.hexm
 */
public class CobarDetectorHandler implements NIOHandler {
    private static final Logger LOGGER = Logger.getLogger(CobarDetectorHandler.class);

    private final CobarDetector detector;
    private final BlockingQueue<byte[]> dataQueue;
    private final AtomicBoolean handleStatus;
    private boolean isFirstPacket = true;
    private int eofPacketCount = 0;

    public CobarDetectorHandler(CobarDetector detector) {
        this.detector = detector;
        this.dataQueue = new LinkedBlockingQueue<byte[]>();
        this.handleStatus = new AtomicBoolean(false);
    }

    @Override
    public void handle(byte[] data) {
        if (dataQueue.offer(data)) {
            handleQueue();
        } else {
            this.isFirstPacket = true;
            this.eofPacketCount = 0;
            throw new HeartbeatException("Add data to queue failure");
        }
    }

    /**
     * 处理队列
     */
    private void handleQueue() {
        if (handleStatus.compareAndSet(false, true)) {
            detector.getProcessor().getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        handleData();
                    } catch (Throwable e) {
                        LOGGER.warn(detector.toString(), e);
                        setStatus(CobarHeartbeat.ERROR_STATUS, null);
                    }
                }
            });
        }
    }

    /**
     * 处理数据
     */
    private void handleData() {
        try {
            byte[] data = null;
            while ((data = dataQueue.poll()) != null) {
                if (isFirstPacket) {
                    handleFirst(data);
                } else {
                    handleNext(data);
                }
            }
        } finally {
            handleStatus.set(false);
            if (dataQueue.size() > 0) {
                handleQueue();
            }
        }
    }

    /**
     * 处理首个数据包
     */
    private void handleFirst(byte[] data) {
        switch (data[4]) {
        case OkPacket.FIELD_COUNT://end
            setStatus(CobarHeartbeat.OK_STATUS, data);
            break;
        case ErrorPacket.FIELD_COUNT://end
            ErrorPacket err = new ErrorPacket();
            err.read(data);
            switch (err.errno) {
            case ErrorCode.ER_SERVER_SHUTDOWN:
                setStatus(CobarHeartbeat.OFF_STATUS, err.message);
                break;
            default:
                throw new HeartbeatException(new String(err.message));
            }
            break;
        default://rs_header
            isFirstPacket = false;
        }
    }

    /**
     * 处理接下来的数据包
     */
    private void handleNext(byte[] data) {
        switch (data[4]) {
        case ErrorPacket.FIELD_COUNT://end
            ErrorPacket err = new ErrorPacket();
            err.read(data);
            switch (err.errno) {
            case ErrorCode.ER_SERVER_SHUTDOWN:
                setStatus(CobarHeartbeat.OFF_STATUS, null);
                break;
            default:
                throw new HeartbeatException(new String(err.message));
            }
            break;
        case EOFPacket.FIELD_COUNT:
            if (++eofPacketCount == 2) {//end
                setStatus(CobarHeartbeat.OK_STATUS, null);
            }
            break;
        }
    }

    /**
     * 设置判定结果状态
     */
    private void setStatus(int status, byte[] message) {
        isFirstPacket = true;
        eofPacketCount = 0;
        detector.getHeartbeat().setResult(status, detector, false, message);
    }

}
