package com.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.apache.log4j.Logger;

public class DBDMLStatement extends DBStatement {
	
	public static Logger LOG=Logger.getLogger(DBDMLStatement.class);
	private Object genKey;
	public Object mappingId;

	public DBDMLStatement(String SQL, List params) {
		this.SQL = SQL;
		this.params = params;
	}

	public void execute(Connection conn) throws Exception {
		stmt = conn.prepareStatement(this.SQL, Statement.RETURN_GENERATED_KEYS);
		setStatementParams();
		stmt.executeUpdate();
		ResultSet rs = stmt.getGeneratedKeys();
		if (rs.next()) {
			genKey = rs.getObject(1);
		}
	}

	public Object getGenKey() throws Exception {
		if(stmt==null) {
		   LOG.debug("Dependent statment not executed:"+SQL);	
		   throw new Exception("Dependent statment not executed:");
		}
		return genKey;
	}
}
