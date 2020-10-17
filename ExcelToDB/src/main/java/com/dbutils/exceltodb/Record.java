/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dbutils.exceltodb;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Praveen
 */
public class Record {
   private Map<String,Object> columns=new HashMap<>();
   public void addValue(String columnName,Object value){
       columns.put(columnName, value);
   }

    public Map<String, Object> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, Object> columns) {
        this.columns = columns;
    }
  
}
