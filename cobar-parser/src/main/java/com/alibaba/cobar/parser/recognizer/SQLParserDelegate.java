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
 * (created at 2011-6-17)
 */
package com.alibaba.cobar.parser.recognizer;

import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.recognizer.lexer.SQLLexer;
import com.alibaba.cobar.parser.recognizer.syntax.DALParser;
import com.alibaba.cobar.parser.recognizer.syntax.DDLParser;
import com.alibaba.cobar.parser.recognizer.syntax.DMLCallParser;
import com.alibaba.cobar.parser.recognizer.syntax.DMLDeleteParser;
import com.alibaba.cobar.parser.recognizer.syntax.DMLInsertParser;
import com.alibaba.cobar.parser.recognizer.syntax.DMLReplaceParser;
import com.alibaba.cobar.parser.recognizer.syntax.DMLSelectParser;
import com.alibaba.cobar.parser.recognizer.syntax.DMLUpdateParser;
import com.alibaba.cobar.parser.recognizer.syntax.MTSParser;
import com.alibaba.cobar.parser.recognizer.syntax.SQLExprParser;
import com.alibaba.cobar.parser.recognizer.syntax.SQLParser;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public final class SQLParserDelegate {

    private static enum SpecialIdentifier {
        ROLLBACK,
        SAVEPOINT,
        TRUNCATE
    }

    private static final Map<String, SpecialIdentifier> specialIdentifiers = new HashMap<String, SpecialIdentifier>();
    static {
        specialIdentifiers.put("TRUNCATE", SpecialIdentifier.TRUNCATE);
        specialIdentifiers.put("SAVEPOINT", SpecialIdentifier.SAVEPOINT);
        specialIdentifiers.put("ROLLBACK", SpecialIdentifier.ROLLBACK);
    }

    public static SQLStatement parse(SQLLexer lexer, String charset) throws SQLSyntaxErrorException {
        SQLStatement stmt = null;
        boolean isEOF = true;
        SQLExprParser exprParser = new SQLExprParser(lexer, charset);
        stmtSwitch: switch (lexer.token()) {
        case KW_DESC:
        case KW_DESCRIBE:
            stmt = new DALParser(lexer, exprParser).desc();
            break stmtSwitch;
        case KW_SELECT:
        case PUNC_LEFT_PAREN:
            stmt = new DMLSelectParser(lexer, exprParser).selectUnion();
            break stmtSwitch;
        case KW_DELETE:
            stmt = new DMLDeleteParser(lexer, exprParser).delete();
            break stmtSwitch;
        case KW_INSERT:
            stmt = new DMLInsertParser(lexer, exprParser).insert();
            break stmtSwitch;
        case KW_REPLACE:
            stmt = new DMLReplaceParser(lexer, exprParser).replace();
            break stmtSwitch;
        case KW_UPDATE:
            stmt = new DMLUpdateParser(lexer, exprParser).update();
            break stmtSwitch;
        case KW_CALL:
            stmt = new DMLCallParser(lexer, exprParser).call();
            break stmtSwitch;
        case KW_SET:
            stmt = new DALParser(lexer, exprParser).set();
            break stmtSwitch;
        case KW_SHOW:
            stmt = new DALParser(lexer, exprParser).show();
            break stmtSwitch;
        case KW_ALTER:
        case KW_CREATE:
        case KW_DROP:
        case KW_RENAME:
            stmt = new DDLParser(lexer, exprParser).ddlStmt();
            isEOF = false;
            break stmtSwitch;
        case KW_RELEASE:
            stmt = new MTSParser(lexer).release();
            break stmtSwitch;
        case IDENTIFIER:
            SpecialIdentifier si = null;
            if ((si = specialIdentifiers.get(lexer.stringValueUppercase())) != null) {
                switch (si) {
                case TRUNCATE:
                    stmt = new DDLParser(lexer, exprParser).truncate();
                    break stmtSwitch;
                case SAVEPOINT:
                    stmt = new MTSParser(lexer).savepoint();
                    break stmtSwitch;
                case ROLLBACK:
                    stmt = new MTSParser(lexer).rollback();
                    break stmtSwitch;
                }
            }
        default:
            throw new SQLSyntaxErrorException("sql is not a supported statement");
        }
        if (isEOF) {
            while (lexer.token() == Token.PUNC_SEMICOLON) {
                lexer.nextToken();
            }
            if (lexer.token() != Token.EOF) {
                throw new SQLSyntaxErrorException("SQL syntax error!");
            }
        }
        return stmt;
    }

    public static SQLStatement parse(String sql, String charset) throws SQLSyntaxErrorException {
        return parse(new SQLLexer(sql), charset);
    }

    public static SQLStatement parse(String sql) throws SQLSyntaxErrorException {
        return parse(sql, SQLParser.DEFAULT_CHARSET);
    }
}
