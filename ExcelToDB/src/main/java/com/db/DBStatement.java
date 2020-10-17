package com.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

public abstract class DBStatement {
	
	public static Logger LOG=Logger.getLogger(DBStatement.class);
	
	public String SQL;
	public List params;
	public PreparedStatement stmt;
	public List originalParams;
	
	public void close() {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
				LOG.error("Error occured while closing statement", e);
			}
		}
	}
	
	public void setStatementParams() throws Exception, SQLException {
		originalParams=new ArrayList();
		if (params != null && params.size() > 0) {
			int i = 1;
			for (Iterator iterator = params.iterator(); iterator.hasNext();) {
				Object param = iterator.next();
				if(param instanceof DBDMLStatement) {
					Object genKey=((DBDMLStatement)param).getGenKey();
					if(genKey!=null) {
					  stmt.setObject(i++, genKey);
					  originalParams.add(genKey);
					}else {
						throw new Exception("Param is null");
					}
					
				}else if(param instanceof DBSingleValueQuery) {
					Object value=((DBSingleValueQuery)param).getValue();
					if(value!=null) {
					  stmt.setObject(i++, value);
					  originalParams.add(value);
					}else {
						throw new Exception("Param is null");
					}
					
				}else {
					stmt.setObject(i++, param); 
					originalParams.add(param);
				}
			}
		}
	}
	
	public abstract void execute(Connection con) throws Exception;
}
