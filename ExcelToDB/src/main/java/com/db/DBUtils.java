/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.db;

import java.util.Collection;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Praveen
 */
public class DBUtils {
    public static String PREP_INSERT_STMT="insert into $TABLE_NAME$($COLUMNS$) values($VALUES$)";
    public static String generateInsertStatement(String tableName,Collection<String> columns){
        String stmt=new String(PREP_INSERT_STMT);
        stmt=stmt.replace("$TABLE_NAME$", tableName);
        stmt=stmt.replace("$COLUMNS$",String.join(",",columns));
        stmt=stmt.replace("$VALUES$",StringUtils.chop(StringUtils.repeat("?,", columns.size())));
        return stmt;
    }
}
