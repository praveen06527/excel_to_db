package com.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

public class DBSingleValueQuery extends DBStatement {
	
	public static Logger LOG=Logger.getLogger(DBSingleValueQuery.class);
	
	private Object value;
	
	public DBSingleValueQuery(String SQL, List params) {
		this.SQL = SQL;
		this.params = params;
	}
	
	public DBSingleValueQuery(String SQL, Object... params) {
		this.SQL = SQL;
		this.params =Arrays.asList(params);
	}
	
	public void execute(Connection conn) throws Exception {
		stmt = conn.prepareStatement(SQL);
		setStatementParams();
        ResultSet rs=stmt.executeQuery();
		if (rs.next()) {
			value = rs.getObject(1);
		}
	}
	
	public Object getValue() throws Exception {
		if(stmt==null) {
		   throw new Exception("Dependent statment not executed");
		}
		return value;
	}
}
