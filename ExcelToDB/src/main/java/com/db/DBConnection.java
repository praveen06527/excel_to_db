package com.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author Praveen
 */
public class DBConnection {

    public static Connection getConnection(String url,String username,String password) throws SQLException{
    	    Connection con=null;
            try {
            	DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
            	con = DriverManager.getConnection(url, username, password);
            	//con = DriverManager.getConnection("jdbc:sqlserver://HDCDCPSITRIG1\\SQL;databaseName=Kony_Sit_Rig1", "konyuser", "D0tn3tpw");
            	//con = DriverManager.getConnection("jdbc:sqlserver://DPSDCPPILOT\\SQL;databaseName=DCP_Pilot_RigDB", "konyuser", "D0tn3tpw");
                con.setAutoCommit(false);
            } catch(Exception e){
                throw new SQLException(e.getMessage());
            }
            return con;
    }
}
