package org.com.sabs;


import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

public class SnowflakeConnector {
	
	final static Logger logger = Logger.getLogger(SnowflakeConnector.class.getName());
	public static void connectSnowflake(String url, String user,String password, String db, String table, String query,int sourceCount, int recordCount,String targetPath) throws SQLException, ClassNotFoundException, FileNotFoundException {
		int count = 0;
		try (Connection con = DataValidator.getConnection("snowflake", url, user,password)){
			try (Statement stmt = con.createStatement()) {
				System.out.println("Snowflake connection established");
				logger.info("Snowflake connection established");
				try (ResultSet rs = stmt.executeQuery("select count(*) from " + " " + table)) {					
					while (rs.next()) {
						count = rs.getInt(1);
					}
					
				if (recordCount == 0 || recordCount > count)
					recordCount = count;
				
				System.out.println("row count in Snowflake : " + count);
				logger.info("row count in Snowflake : " + count);
				
				if (query != null) {
					query = query.replace("db.table", table);
		
					try (ResultSet rset = stmt.executeQuery(query)) {
						if (rset != null) {
							DataValidator.printToFile(rset, targetPath, recordCount);
						}
					}
				}
			}
		}
	}
  }

