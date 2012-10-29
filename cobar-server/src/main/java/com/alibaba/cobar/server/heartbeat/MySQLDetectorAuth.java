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
import com.alibaba.cobar.net.packet.EOFPacket;
import com.alibaba.cobar.net.packet.ErrorPacket;
import com.alibaba.cobar.net.packet.HandshakePacket;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.net.packet.Reply323Packet;
import com.alibaba.cobar.net.util.CharsetUtil;
import com.alibaba.cobar.net.util.SecurityUtil;

/**
 * @author xianmao.hexm
 */
public class MySQLDetectorAuth implements NIOHandler {
    private final MySQLDetector detector;

    public MySQLDetectorAuth(MySQLDetector detector) {
        this.detector = detector;
    }

    @Override
    public void handle(byte[] data) {
        MySQLDetector detector = this.detector;
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
        } else {
            switch (data[4]) {
            case OkPacket.FIELD_COUNT:
                detector.setHandler(new MySQLDetectorHandler(detector));
                detector.setAuthenticated(true);
                detector.heartbeat();// 成功后发起心跳。
                break;
            case ErrorPacket.FIELD_COUNT:
                ErrorPacket err = new ErrorPacket();
                err.read(data);
                throw new RuntimeException(new String(err.message));
            case EOFPacket.FIELD_COUNT:
                auth323(data[3], hsp.seed);
                break;
            default:
                throw new RuntimeException("Unknown packet");
            }
        }
    }

    /**
     * 发送323响应认证数据包
     */
    private void auth323(byte packetId, byte[] seed) {
        Reply323Packet r323 = new Reply323Packet();
        r323.packetId = ++packetId;
        String pass = detector.getPassword();
        if (pass != null && pass.length() > 0) {
            r323.seed = SecurityUtil.scramble323(pass, new String(seed)).getBytes();
        }
        r323.write(detector);
    }

}
