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

import com.alibaba.cobar.net.handler.NIOHandler;
import com.alibaba.cobar.net.packet.ErrorPacket;
import com.alibaba.cobar.net.packet.HandshakePacket;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.net.util.CharsetUtil;

/**
 * @author xianmao.hexm
 */
public class CobarDetectorAuth implements NIOHandler {

    private final CobarDetector detector;

    public CobarDetectorAuth(CobarDetector detector) {
        this.detector = detector;
    }

    @Override
    public void handle(byte[] data) {
        CobarDetector detector = this.detector;
        HandshakePacket hsp = detector.getHandshake();
        if (hsp == null) {
            // 设置握手数据包
            hsp = new HandshakePacket();
            hsp.read(data);
            detector.setHandshake(hsp);

            // 设置字符集编码
            int charsetIndex = (hsp.serverCharsetIndex & 0xff);
            String charset = CharsetUtil.getCharset(charsetIndex);
            if (charset != null) {
                detector.setCharsetIndex(charsetIndex);
            } else {
                throw new RuntimeException("Unknown charsetIndex:" + charsetIndex);
            }

            // 发送认证数据包
            detector.authenticate();
        } else { // 处理认证结果
            switch (data[4]) {
            case OkPacket.FIELD_COUNT:
                detector.setHandler(new CobarDetectorHandler(detector));
                detector.setAuthenticated(true);
                detector.heartbeat();// 认证成功后，发起心跳。
                break;
            case ErrorPacket.FIELD_COUNT:
                ErrorPacket err = new ErrorPacket();
                err.read(data);
                throw new RuntimeException(new String(err.message));
            default:
                throw new RuntimeException("Unknown packet");
            }
        }
    }

}
