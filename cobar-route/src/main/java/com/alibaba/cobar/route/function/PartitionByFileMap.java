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
 * (created at 2011-11-21)
 */
package com.alibaba.cobar.route.function;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class PartitionByFileMap extends FunctionExpression {
    public PartitionByFileMap(String functionName, List<Expression> arguments) {
        super(functionName, arguments);
    }

    private Integer defaultNode;
    private String fileMapPath;

    public void setDefaultNode(Integer defaultNode) {
        this.defaultNode = defaultNode;
    }

    public void setFileMapPath(String fileMapPath) {
        this.fileMapPath = fileMapPath;
    }

    private Map<String, Integer> app2Partition;

    @Override
    public void init() {
        InputStream fin = null;
        try {
            fin = new FileInputStream(new File(fileMapPath));
            BufferedReader in = new BufferedReader(new InputStreamReader(fin));
            app2Partition = new HashMap<String, Integer>();
            for (String line = null; (line = in.readLine()) != null;) {
                line = line.trim();
                if (line.startsWith("#") || line.startsWith("//")) continue;
                int ind = line.indexOf('=');
                if (ind < 0) continue;
                try {
                    String key = line.substring(0, ind).trim();
                    int pid = Integer.parseInt(line.substring(ind + 1).trim());
                    app2Partition.put(key, pid);
                } catch (Exception e) {
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                fin.close();
            } catch (Exception e2) {
            }
        }
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        Object arg = arguments.get(0).evaluation(parameters);
        if (arg == null) {
            throw new IllegalArgumentException("partition key is null ");
        } else if (arg == UNEVALUATABLE) {
            throw new IllegalArgumentException("argument is UNEVALUATABLE");
        }
        Integer pid = app2Partition.get(arg);
        if (pid == null) return defaultNode;
        return pid;
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        if (arguments == null || arguments.size() != 1)
            throw new IllegalArgumentException("function "
                                               + getFunctionName()
                                               + " must have 1 arguments but is "
                                               + arguments);
        PartitionByFileMap rst = new PartitionByFileMap(functionName, arguments);
        rst.fileMapPath = fileMapPath;
        rst.defaultNode = defaultNode;
        return rst;
    }

}
