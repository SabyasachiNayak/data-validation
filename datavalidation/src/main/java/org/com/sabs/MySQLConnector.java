package org.com.sabs;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class MySQLConnector {
	final static Logger logger = Logger.getLogger(MySQLConnector.class.getName());
	
	
	public static int getMySQLRecordCount(String url, String user,String password, String db, String table, int recordCount, String query, String sourcePath) throws SQLException, ClassNotFoundException, FileNotFoundException {
		int count = 0;
		try (Connection con = DataValidator.getConnection("mysql", "jdbc:mysql://" + url, user,password)){
			try (Statement stmt = con.createStatement()) {
				System.out.println("mysql connection established");
				logger.info("mysql connection established");
				
				try (ResultSet rs = stmt.executeQuery("select count(*) from " + " " + db + "." + table)) {
					
					while (rs.next()) {
						count = rs.getInt(1);
					}
					
				if (recordCount == 0 || recordCount > count)
					recordCount = count;
				
				System.out.println("row count in mysql : " + count);
				logger.info("row count in mysql : " + count);
				
				if (query != null) {
					query = query.replace("db.table", db + "." + table);
					try (ResultSet rset = stmt.executeQuery(query)) {
						if (rset != null) {
							DataValidator.printToFile(rset, sourcePath, recordCount);
						}
					}
				}
			}
		}
		return count;
	}
  }
}
