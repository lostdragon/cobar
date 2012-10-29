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
 * (created at 2011-7-4)
 */
package com.alibaba.cobar.parser.recognizer.syntax;

import static com.alibaba.cobar.parser.recognizer.Token.KW_EXISTS;
import static com.alibaba.cobar.parser.recognizer.Token.KW_IF;
import static com.alibaba.cobar.parser.recognizer.Token.KW_IGNORE;
import static com.alibaba.cobar.parser.recognizer.Token.KW_NOT;
import static com.alibaba.cobar.parser.recognizer.Token.KW_ON;
import static com.alibaba.cobar.parser.recognizer.Token.KW_TABLE;
import static com.alibaba.cobar.parser.recognizer.Token.KW_TO;
import static com.alibaba.cobar.parser.recognizer.Token.PUNC_COMMA;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.stmt.ddl.DDLAlterTableStatement;
import com.alibaba.cobar.parser.ast.stmt.ddl.DDLCreateIndexStatement;
import com.alibaba.cobar.parser.ast.stmt.ddl.DDLCreateTableStatement;
import com.alibaba.cobar.parser.ast.stmt.ddl.DDLDropIndexStatement;
import com.alibaba.cobar.parser.ast.stmt.ddl.DDLDropTableStatement;
import com.alibaba.cobar.parser.ast.stmt.ddl.DDLRenameTableStatement;
import com.alibaba.cobar.parser.ast.stmt.ddl.DDLStatement;
import com.alibaba.cobar.parser.ast.stmt.ddl.DDLTruncateStatement;
import com.alibaba.cobar.parser.recognizer.lexer.SQLLexer;
import com.alibaba.cobar.parser.util.Pair;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class DDLParser extends SQLParser {
    protected SQLExprParser exprParser;

    public DDLParser(SQLLexer lexer, SQLExprParser exprParser) {
        super(lexer);
        this.exprParser = exprParser;
    }

    private static enum SpecialIdentifier {
        TRUNCATE,
        TEMPORARY,
        DEFINER
    }

    private static final Map<String, SpecialIdentifier> specialIdentifiers = new HashMap<String, SpecialIdentifier>();
    static {
        specialIdentifiers.put("TRUNCATE", SpecialIdentifier.TRUNCATE);
        specialIdentifiers.put("TEMPORARY", SpecialIdentifier.TEMPORARY);
        specialIdentifiers.put("DEFINER", SpecialIdentifier.DEFINER);
    }

    public DDLTruncateStatement truncate() throws SQLSyntaxErrorException {
        matchIdentifier("TRUNCATE");
        if (lexer.token() == KW_TABLE) {
            lexer.nextToken();
        }
        Identifier tb = identifier();
        return new DDLTruncateStatement(tb);
    }

    /**
     * nothing has been pre-consumed
     */
    public DDLStatement ddlStmt() throws SQLSyntaxErrorException {
        Identifier idTemp1;
        Identifier idTemp2;
        SpecialIdentifier siTemp;
        switch (lexer.token()) {
        case KW_ALTER:
            boolean ignore = false;
            if (lexer.nextToken() == KW_IGNORE) {
                ignore = true;
                lexer.nextToken();
            }
            switch (lexer.token()) {
            case KW_TABLE:
                lexer.nextToken();
                idTemp1 = identifier();
                return new DDLAlterTableStatement(ignore, idTemp1);
            default:
                throw err("Only ALTER TABLE is supported");
            }
        case KW_CREATE:
            switch (lexer.nextToken()) {
            case KW_UNIQUE:
            case KW_FULLTEXT:
            case KW_SPATIAL:
                lexer.nextToken();
            case KW_INDEX:
                lexer.nextToken();
                idTemp1 = identifier();
                for (; lexer.token() != KW_ON; lexer.nextToken());
                lexer.nextToken();
                idTemp2 = identifier();
                return new DDLCreateIndexStatement(idTemp1, idTemp2);
            case KW_TABLE:
                lexer.nextToken();
                return createTable(false);
            case IDENTIFIER:
                siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
                if (siTemp != null) {
                    switch (siTemp) {
                    case TEMPORARY:
                        lexer.nextToken();
                        match(KW_TABLE);
                        return createTable(true);
                    }
                }
            default:
                throw err("unsupported DDL for CREATE");
            }
        case KW_DROP:
            switch (lexer.nextToken()) {
            case KW_INDEX:
                lexer.nextToken();
                idTemp1 = identifier();
                match(KW_ON);
                idTemp2 = identifier();
                return new DDLDropIndexStatement(idTemp1, idTemp2);
            case KW_TABLE:
                lexer.nextToken();
                return dropTable(false);
            case IDENTIFIER:
                siTemp = specialIdentifiers.get(lexer.stringValueUppercase());
                if (siTemp != null) {
                    switch (siTemp) {
                    case TEMPORARY:
                        lexer.nextToken();
                        match(KW_TABLE);
                        return dropTable(true);
                    }
                }
            default:
                throw err("unsupported DDL for DROP");
            }
        case KW_RENAME:
            lexer.nextToken();
            match(KW_TABLE);
            idTemp1 = identifier();
            match(KW_TO);
            idTemp2 = identifier();
            List<Pair<Identifier, Identifier>> list;
            if (lexer.token() != PUNC_COMMA) {
                list = new ArrayList<Pair<Identifier, Identifier>>(1);
                list.add(new Pair<Identifier, Identifier>(idTemp1, idTemp2));
                return new DDLRenameTableStatement(list);
            }
            list = new LinkedList<Pair<Identifier, Identifier>>();
            list.add(new Pair<Identifier, Identifier>(idTemp1, idTemp2));
            for (; lexer.token() == PUNC_COMMA;) {
                lexer.nextToken();
                idTemp1 = identifier();
                match(KW_TO);
                idTemp2 = identifier();
                list.add(new Pair<Identifier, Identifier>(idTemp1, idTemp2));
            }
            return new DDLRenameTableStatement(list);
        case IDENTIFIER:
            SpecialIdentifier si = specialIdentifiers.get(lexer.stringValueUppercase());
            if (si != null) {
                switch (si) {
                case TRUNCATE:
                    return truncate();
                }
            }
        default:
            throw err("unsupported DDL");
        }
    }

    /**
     * <code>TABLE</code> has been consumed
     */
    private DDLDropTableStatement dropTable(boolean temp) throws SQLSyntaxErrorException {
        boolean ifExists = false;
        if (lexer.token() == KW_IF) {
            lexer.nextToken();
            match(KW_EXISTS);
            ifExists = true;
        }
        Identifier tb = identifier();
        List<Identifier> list;
        if (lexer.token() != PUNC_COMMA) {
            list = new ArrayList<Identifier>(1);
            list.add(tb);
        } else {
            list = new LinkedList<Identifier>();
            list.add(tb);
            for (; lexer.token() == PUNC_COMMA;) {
                lexer.nextToken();
                tb = identifier();
                list.add(tb);
            }
        }
        DDLDropTableStatement.Mode mode = DDLDropTableStatement.Mode.UNDEF;
        switch (lexer.token()) {
        case KW_RESTRICT:
            lexer.nextToken();
            mode = DDLDropTableStatement.Mode.RESTRICT;
            break;
        case KW_CASCADE:
            lexer.nextToken();
            mode = DDLDropTableStatement.Mode.CASCADE;
            break;
        }
        return new DDLDropTableStatement(list, temp, ifExists, mode);
    }

    /**
     * <code>TABLE</code> has been consumed
     */
    private DDLCreateTableStatement createTable(boolean temp) throws SQLSyntaxErrorException {
        if (lexer.token() == KW_IF) {
            lexer.nextToken();
            match(KW_NOT);
            match(KW_EXISTS);
        }
        Identifier table = identifier();
        return new DDLCreateTableStatement(table);
    }

}
