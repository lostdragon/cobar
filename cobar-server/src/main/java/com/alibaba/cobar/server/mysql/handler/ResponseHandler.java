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

import com.alibaba.cobar.net.packet.ErrorPacket;
import com.alibaba.cobar.server.mysql.MySQLConnection;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public interface ResponseHandler {
    /**
     * NEVER throw any {@link Throwable}<br/>
     * return quickly
     */
    void connectionAcquired(MySQLConnection conn);

    /**
     * last invocation of {@link ResponseHandler} on a {@link MySQLConnection}
     */
    void connectionError(Throwable e, MySQLConnection conn);

    void okPacket(byte[] ok, MySQLConnection conn);

    void fieldsEnd(byte[] header, byte[][] fields, byte[] eof, MySQLConnection conn);

    void rowAquired(byte[] row, MySQLConnection conn);

    void rowEnd(byte[] eof, MySQLConnection conn);

    void errorPacket(ErrorPacket err, MySQLConnection conn);

}
