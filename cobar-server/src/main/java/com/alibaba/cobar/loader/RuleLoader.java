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
 * (created at 2011-7-13)
 */
package com.alibaba.cobar.loader;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.alibaba.cobar.config.util.ConfigException;
import com.alibaba.cobar.config.util.ConfigUtil;
import com.alibaba.cobar.config.util.ParameterMapping;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;
import com.alibaba.cobar.parser.recognizer.FunctionManager;
import com.alibaba.cobar.parser.recognizer.Token;
import com.alibaba.cobar.parser.recognizer.lexer.SQLLexer;
import com.alibaba.cobar.parser.recognizer.syntax.SQLExprParser;
import com.alibaba.cobar.parser.recognizer.syntax.SQLParser;
import com.alibaba.cobar.route.config.TableRuleConfig;
import com.alibaba.cobar.route.config.TableRuleConfig.RuleConfig;
import com.alibaba.cobar.util.SplitUtil;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public final class RuleLoader {

    private final Map<String, TableRuleConfig> tableRules;
    private final FunctionManager functionManager;

    public RuleLoader() {
        this.tableRules = new HashMap<String, TableRuleConfig>();
        this.functionManager = new FunctionManager(true);
        this.load();
    }

    public Map<String, TableRuleConfig> getTableRules() {
        return tableRules;
    }

    private void load() {
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = RuleLoader.class.getResourceAsStream("/rule.dtd");
            xml = RuleLoader.class.getResourceAsStream("/rule.xml");
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            loadFunctions(root);
            loadTableRules(root);
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException(e);
        } finally {
            if (dtd != null) {
                try {
                    dtd.close();
                } catch (IOException e) {
                }
            }
            if (xml != null) {
                try {
                    xml.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private void loadFunctions(Element root) throws ClassNotFoundException, InstantiationException,
            IllegalAccessException, InvocationTargetException {
        NodeList list = root.getElementsByTagName("function");
        Map<String, FunctionExpression> functions = new HashMap<String, FunctionExpression>();
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getAttribute("name");
                if (functions.containsKey(name)) {
                    throw new ConfigException("rule function " + name + " duplicated!");
                }
                String clazz = e.getAttribute("class");
                FunctionExpression function = createFunction(name, clazz);
                ParameterMapping.mapping(function, ConfigUtil.loadElements(e));
                functions.put(name, function);
            }
        }
        functionManager.addExtendFunction(functions);
    }

    @SuppressWarnings("unchecked")
    private FunctionExpression createFunction(String name, String clazz) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, InvocationTargetException {
        Class<?> clz = Class.forName(clazz);
        if (!FunctionExpression.class.isAssignableFrom(clz)) {
            throw new ConfigException("function " + name + " dose not extend " + FunctionExpression.class.getName());
        }
        Constructor<? extends FunctionExpression> constructor = null;
        for (Constructor<?> cons : clz.getConstructors()) {
            Class<?>[] paraClzs = cons.getParameterTypes();
            if (paraClzs != null && paraClzs.length == 2) {
                Class<?> paraClzs1 = paraClzs[0];
                Class<?> paraClzs2 = paraClzs[1];
                if (String.class.isAssignableFrom(paraClzs1)
                    && (Object[].class.isAssignableFrom(paraClzs2) || List.class.isAssignableFrom(paraClzs2))) {
                    constructor = (Constructor<? extends FunctionExpression>) cons;
                    break;
                }
            }
        }
        if (constructor == null) {
            throw new ConfigException(
                    "function "
                            + name
                            + " with class of "
                            + clazz
                            + " must have a constructor with two parameters: String funcName, Object[] params");
        }
        return constructor.newInstance(name, null);
    }

    private void loadTableRules(Element root) throws SQLSyntaxErrorException {
        NodeList list = root.getElementsByTagName("tableRule");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getAttribute("name");
                if (tableRules.containsKey(name)) {
                    throw new ConfigException("table rule " + name + " duplicated!");
                }
                NodeList ruleList = e.getElementsByTagName("rule");
                int length = ruleList.getLength();
                RuleConfig[] rules = new RuleConfig[length];
                for (int j = 0; j < length; ++j) {
                    rules[j] = loadRule((Element) ruleList.item(j));
                }
                tableRules.put(name, new TableRuleConfig(name, rules));
            }
        }
    }

    private RuleConfig loadRule(Element element) throws SQLSyntaxErrorException {
        Element columnsEle = ConfigUtil.loadElement(element, "columns");
        String[] columns = SplitUtil.split(columnsEle.getTextContent(), ',', true);
        for (int i = 0; i < columns.length; ++i) {
            columns[i] = columns[i].toUpperCase();
        }
        Element algorithmEle = ConfigUtil.loadElement(element, "algorithm");
        String algorithm = algorithmEle.getTextContent();
        SQLLexer lexer = new SQLLexer(algorithm);
        SQLExprParser parser = new SQLExprParser(lexer, functionManager, false, SQLParser.DEFAULT_CHARSET);
        Expression expression = parser.expression();
        if (lexer.token() != Token.EOF) {
            throw new ConfigException("route algorithm not end with EOF: " + algorithm);
        }
        return new RuleConfig(columns, expression);
    }

}
