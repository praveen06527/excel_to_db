package com.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

/**
 *
 * @author Praveen
 */
public class DBOperations {
	public static Logger LOG=Logger.getLogger(DBOperations.class);
	private Connection conn=null;
	
	public DBOperations(Connection con) {
		this.conn=con;
	}
	
	public void startTransaction() {
		try {
			this.conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
    public void executeTransaction(List<DBStatement> dbstmts) throws Exception{
    	DBStatement currStmt = null;
    	try {
	    	this.conn.setAutoCommit(false);
	    	for(DBStatement stmt:dbstmts) {
	    		currStmt=stmt;
	    		stmt.execute(this.conn);
	    	}
	    	this.conn.commit();	
    	}catch(Exception e) {
    		try {
    			this.conn.rollback();	
    			LOG.debug("transaction rolledback:"+currStmt.SQL);
    		}catch (Exception ex) {
    			LOG.error("Rollback failed", ex);
			}
    		throw e;
    	}finally {
    		for(DBStatement stmt:dbstmts) {
	    		stmt.close();
	    	}	
    		this.conn.setAutoCommit(true);
    	}	
    }
    
    public ResultSet query(String stmt,List params) throws Exception{
        PreparedStatement prepareStmt = conn.prepareStatement(stmt,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
        if(params!=null && params.size()>0){
            int i=1;
            for (Iterator iterator = params.iterator(); iterator.hasNext();) {
                Object next = iterator.next();
                if(next instanceof DBSingleValueQuery) {
					((DBSingleValueQuery)next).execute(conn);
					next=((DBSingleValueQuery)next).getValue();
				};
                prepareStmt.setObject(i++, next);
            }
        }
        return prepareStmt.executeQuery();
    }
 
    public Object getInClauseArray(String type,Object[] elements) throws SQLException{
            return conn.createArrayOf(type, elements);
    }
    
    public void closeConnection() {
    	try {
			conn.close();
		} catch (SQLException e) {
			LOG.error("Error occured while closing connection",e);
		}
    }
}
