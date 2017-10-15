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

	public static void main1(String[] args) throws Exception
	  {
	    // get connection
	    System.out.println("Create JDBC connection");
	    Connection connection = getConnection();
	    System.out.println("Done creating JDBC connectionn");
	    // create statement
	    System.out.println("Create JDBC statement");
	    Statement statement = connection.createStatement();
	    System.out.println("Done creating JDBC statementn");
	    // create a table
	    System.out.println("Create demo table");
	    statement.executeUpdate("create or replace table demo(C1 STRING)");
	    statement.close();
	    System.out.println("Done creating demo table");
	    // insert a row
	    System.out.println("Insert 'hello world'");
	    statement.executeUpdate("insert into demo values ('hello world')");
	    statement.close();
	    System.out.println("Done inserting 'hello world'n");
	    // query the data
	    System.out.println("Query demo");
	    ResultSet resultSet = statement.executeQuery("SELECT * FROM demo");
	    System.out.println("Metadata:");
	    System.out.println("================================");
	    // fetch metadata
	    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
	    System.out.println("Number of columns=" +
	                       resultSetMetaData.getColumnCount());
	    for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount();
	                         colIdx++)
	    {
	      System.out.println("Column " + colIdx + ": type=" +
	                         resultSetMetaData.getColumnTypeName(colIdx+1));
	    }
	    // fetch data
	    System.out.println("nData:");
	    System.out.println("================================");
	    int rowIdx = 0;
	    while(resultSet.next())
	    {
	      System.out.println("row " + rowIdx + ", column 0: " +
	                         resultSet.getString(1));
	    }
	    statement.close();
	  }
	
	   private static Connection getConnection()
	          throws SQLException
	   {
	    try
	    {
	      Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
	    }
	    catch (ClassNotFoundException ex)
	    {
	     System.err.println("Driver not found");
	    }
	    // build connection properties
	    Properties properties = new Properties();
	    //properties.put("account", "tataconsulting");  // replace "" with your account name
	    properties.put("user", "sabyasachinayak");     // replace "" with your username
	    properties.put("password", "12345"); // replace "" with your password
	    properties.put("warehouse", "DEMO_WH");
	    properties.put("db", "TEST_DB");       // replace "" with target database name
	    properties.put("schema", "PUBLIC");   // replace "" with target schema name
	    //properties.put("tracing", "on");

	    // create a new connection
	    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
	    // use the default connection string if it is not set in environment
	    if(connectStr == null)
	    {
	     connectStr = "jdbc:snowflake://snowflakecomputing.com"; // replace accountName with your account name
	    }
	    return DriverManager.getConnection(connectStr, properties);
	  }

}
