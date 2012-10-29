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
package com.alibaba.cobar.net.handler;

import com.alibaba.cobar.Commands;
import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.net.FrontendConnection;

/**
 * 前端命令处理器
 * 
 * @author xianmao.hexm
 */
public class FrontendCommandHandler implements NIOHandler {

    protected final FrontendConnection source;
    protected final CommandCount commandCount;

    public FrontendCommandHandler(FrontendConnection source) {
        this.source = source;
        this.commandCount = source.getProcessor().getCommands();
    }

    @Override
    public void handle(byte[] data) {
        switch (data[4]) {
        case Commands.COM_INIT_DB:
            commandCount.doInitDB();
            source.initDB(data);
            break;
        case Commands.COM_QUERY:
            commandCount.doQuery();
            source.query(data);
            break;
        case Commands.COM_PING:
            commandCount.doPing();
            source.ping();
            break;
        case Commands.COM_QUIT:
            commandCount.doQuit();
            source.close();
            break;
        case Commands.COM_PROCESS_KILL:
            commandCount.doKill();
            source.kill(data);
            break;
        case Commands.COM_STMT_PREPARE:
            commandCount.doStmtPrepare();
            source.stmtPrepare(data);
            break;
        case Commands.COM_STMT_EXECUTE:
            commandCount.doStmtExecute();
            source.stmtExecute(data);
            break;
        case Commands.COM_STMT_CLOSE:
            commandCount.doStmtClose();
            source.stmtClose(data);
            break;
        case Commands.COM_HEARTBEAT:
            commandCount.doHeartbeat();
            source.heartbeat(data);
            break;
        default:
            commandCount.doOther();
            source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

}
