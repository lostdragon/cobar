package com.alibaba.cobar.route.util;

import com.alibaba.cobar.route.config.TableConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:dragon829@gmail.com">lostdragon</a>
 */
public class RegexUtil {

    static Map<String, TableConfig> map = new HashMap<String, TableConfig>();

    /**
     * 根据表名获取配置
     * @param tables    表配置关系
     * @param tableName 表名
     * @return 配置
     */
    public static TableConfig get(Map<String, TableConfig> tables, String tableName)
    {
        TableConfig tc = map.get(tableName);
        if(tc == null) {
            tc = tables.get(tableName);
            if(tc == null) {
                ft: for(Entry<String, TableConfig> e :tables.entrySet()) {
                    String key = e.getKey();
                    if(key.indexOf('[') != -1 && Pattern.compile(key).matcher(tableName).find()) {
                        tc = e.getValue();
                        break ft;
                    }
                }
            }
            map.put(tableName, tc);
        }

        return tc;
    }
}

