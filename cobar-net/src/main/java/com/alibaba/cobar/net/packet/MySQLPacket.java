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
package com.alibaba.cobar.net.packet;

import java.nio.ByteBuffer;

import com.alibaba.cobar.net.BackendConnection;
import com.alibaba.cobar.net.FrontendConnection;

/**
 * @author xianmao.hexm
 */
public abstract class MySQLPacket {

    public int packetLength;
    public byte packetId;

    /**
     * 把数据包写到buffer中，如果buffer满了就把buffer通过前端连接写出。
     */
    public ByteBuffer write(ByteBuffer buffer, FrontendConnection c) {
        throw new UnsupportedOperationException();
    }

    /**
     * 把数据包通过后端连接写出，一般使用buffer机制来提高写的吞吐量。
     */
    public void write(BackendConnection c) {
        throw new UnsupportedOperationException();
    }

    /**
     * 计算数据包大小，不包含包头长度。
     */
    public abstract int calcPacketSize();

    /**
     * 取得数据包信息
     */
    protected abstract String getPacketInfo();

    @Override
    public String toString() {
        return new StringBuilder().append(getPacketInfo())
                                  .append("{length=")
                                  .append(packetLength)
                                  .append(",id=")
                                  .append(packetId)
                                  .append('}')
                                  .toString();
    }

}
