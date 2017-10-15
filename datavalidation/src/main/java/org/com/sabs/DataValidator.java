package org.com.sabs;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.log4j.Logger;

public class DataValidator {
	final static Logger logger = Logger.getLogger(DataValidator.class.getName());
	public static void main(String args[]) throws SQLException, ClassNotFoundException, IOException, ParseException {
	Options opt = DataValidator.prepareOptions();
	CommandLineParser parser = new BasicParser();
	CommandLine cmd = parser.parse(opt,args);
	
	String sourceType = cmd.getOptionValue("sourceType");
	String sourceUrl = cmd.getOptionValue("sourceUrl");
	String sourceUser = cmd.getOptionValue("sourceUser");
	String sourcePassword = cmd.getOptionValue("sourcePassword");
	String sourcedb = cmd.getOptionValue("sourcedb");
	String sourceTable = cmd.getOptionValue("sourceTable");
	String targetType = cmd.getOptionValue("targetType");
	String targetUrl = cmd.getOptionValue("targetUrl");
	String targetUser = cmd.getOptionValue("targetUser");
	String targetPassword = cmd.getOptionValue("targetPassword");
	String targetdb = cmd.getOptionValue("targetdb");
	String targetTable = cmd.getOptionValue("targetTable");
	String record = cmd.getOptionValue("record");
	//String env = cmd.getOptionValue("env");
	String query = cmd.getOptionValue("query");
	
	
	int recordCount = 0;
	if (record != null)
		recordCount = Integer.parseInt(record);
	
	// Read properties from configuration file
	/*HashMap<String, String> prop = readPropertiesFile(env);
	
	String hanaurl = prop.get("hanaurl");
	String hanaUser = prop.get("hanaUser");
	String hanaPassword = prop.get("hanaPassword");
	String realm = prop.get("realm");
	String hiveurl = prop.get("hiveurl");*/
	
	// Connect to Source
	String sourcePath = "C:\\demo\\" + sourceType + "_" + sourceTable + ".txt";
	int sourceCount = MySQLConnector.getMySQLRecordCount(sourceUrl, sourceUser, sourcePassword, sourcedb, sourceTable, recordCount, query, sourcePath);
	
	// Generate hash code for source data
	byte[] sourcebytes = Files.readAllBytes(Paths.get(sourcePath));
	String sourceHashCode = HashGenerator.generateHash(sourcebytes);
	System.out.println(sourceType + " hash code " + sourceHashCode);
	logger.info(sourceHashCode + " hash code " + sourceHashCode);
	
	// Connect to Target
	String targetPath = "C:\\demo\\" + targetType + "_" + targetTable + ".txt";
	String targetHashCode = null;
	if(targetType.equalsIgnoreCase("snowflake"))
	//HiveConnector.connectToHive(hiveurl, targetdb, targetTable, query, user, realm, sourceCount, recordCount, targetPath);
	SnowflakeConnector.connectSnowflake(targetUrl,targetUser,targetPassword, targetdb, targetTable, query, sourceCount, recordCount, targetPath);
	
	// Generate hash code for target data
	byte[] targetbytes = Files.readAllBytes(Paths.get(targetPath));
	targetHashCode = HashGenerator.generateHash(targetbytes);
	System.out.println(targetType + " hash code " + targetHashCode);
	logger.info(targetType + " hash code " + targetHashCode);
	
	if (sourceHashCode != null && targetHashCode != null) {
		if (sourceHashCode.equalsIgnoreCase(targetHashCode)){
			System.out.println("Source and Target hashcode matches.");
			System.out.println("Source and target data are same.");
			logger.info("files are identical");
		} else {
			generateReport(sourcePath,targetPath);
		}
		System.out.println("Data Validation completed");
		logger.info("Data Validation completed");
	  } else {
		  System.out.println("Please check data validation again with proper inputs");
		  logger.info("Please check data validation again with proper inputs");
	  }
 }	

	public static Options prepareOptions() {
		Options options = new Options();
		/*options.addOption("hanadb", true, "Hana database");
		options.addOption("hanatable", true, "Hana table");
		options.addOption("hivedb", true, "Hive database");
		options.addOption("hivetable", true, "Hive table");
		options.addOption("records", true, "number of records");
		options.addOption("env", true, "environment");
		options.addOption("query", true, "query");
		options.addOption("user", true, "user");*/
		
		options.addOption("mysqldb", true, "mysql database");
		options.addOption("mysqltable", true, "mysql table");
		options.addOption("sfdb", true, "Snowflake database");
		options.addOption("sftable", true, "Snowflake table");
		
		options.addOption("sourceType", true, "Source Type");
		options.addOption("sourceUrl", true, "Source URL");
		options.addOption("sourcedb", true, "Source database");
		options.addOption("sourceTable", true, "Source Table");
		options.addOption("targetType", true, "Target Type");
		options.addOption("targetUrl", true, "Target URL");
		options.addOption("targetdb", true, "Target database");
		options.addOption("targetTable", true, "Target table");
		options.addOption("records", true, "number of records");
		options.addOption("query", true, "query");
		options.addOption("sourceUser", true, "source user");
		options.addOption("sourcePassword", true, "source password");
		options.addOption("targetUser", true, "target user");
		options.addOption("targetPassword", true, "target password");
		options.addOption("env", true, "environment");
		return options;
	}
	
	public static HashMap<String, String> readPropertiesFile(String env) throws IOException {
		Properties prop = new Properties();
		HashMap<String, String> properties = new HashMap<String,String>();
		String propFileName = "config.properties";
		try (InputStream inputStream = DataValidator.class.getClassLoader().getResourceAsStream(propFileName)) {
			prop.load(inputStream);
		}
		
		properties.put("hanaurl", prop.getProperty("hanaurl"));
		properties.put("hanauser", prop.getProperty("hanauser"));
		properties.put("hanapassword", prop.getProperty("hanapassword"));
		properties.put("realm", prop.getProperty("realm"));
		properties.put("hiveurl", prop.getProperty("hiveurl"));
		
		logger.info("got all properties from property file");
		
		return properties;
	}
	
	public static Connection getConnection(String sourceType, String connectionUrl, String user, String password) throws SQLException, ClassNotFoundException {
		if (sourceType.equalsIgnoreCase("hana")) {
			Class.forName("com.sap.db.jdbc.Driver");
		} else if (sourceType.equalsIgnoreCase("hive")) {
			Class.forName("org.apche.hive.jdbc.HiveDriver");
		}else if (sourceType.equalsIgnoreCase("teradata")) {
			Class.forName("com.teradata.jdbc.TeraDriver");
		}
		else if (sourceType.equalsIgnoreCase("snowflake")) {
			Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
			
			// build connection properties
		    Properties properties = new Properties();
		    properties.put("user", "sabyasachinayak");    
		    properties.put("password", "Scs@12345"); 
		    properties.put("warehouse", "DEMO_WH");
		    properties.put("db", "TEST_DB");       
		    properties.put("schema", "PUBLIC");  
		   
		    // create a new connection
		    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
		    // use the default connection string if it is not set in environment
		    if(connectStr == null)
		    {
		    	connectStr = "jdbc:snowflake://snowflakecomputing.com"; 
		    }
		    return DriverManager.getConnection(connectStr, properties);
		}
		else if (sourceType.equalsIgnoreCase("mysql")) {
			Class.forName("com.mysql.jdbc.Driver");
		}
		
		DriverManager.setLoginTimeout(0);
		
		return DriverManager.getConnection(connectionUrl, user, password);
	}
	
	public static void printToFile(ResultSet rs, String path, int recordCount) throws FileNotFoundException, SQLException {
		try (PrintStream out = new PrintStream(new FileOutputStream(path))) {
			int cols = rs.getMetaData().getColumnCount();
			int count = 0;
			logger.info("printToFile started");
			while (rs.next()) {
				count = count + 1;
				if (count <= recordCount) {
					for (int i = 1; i <= cols; i++) {
						if (i != cols) {
							if (rs.getObject(i) != null && rs.getObject(i).toString().trim() != "") {
								String line = rs.getObject(i).toString().trim();
								if (line.contains(".") && line.endsWith("00"))
									line = line.substring(0, line.length() -3);
								else if (line.contains(".") && line.endsWith("0"))
									line = line.substring(0, line.length() -1);
								out.printf("%s,", line);
							} else {
								out.printf("%s,", "NULL");
							}
						} else {
							if (rs.getObject(i) != null && rs.getObject(i).toString().trim() != "") {
								String line = rs.getObject(i).toString().trim();
								if (line.contains(".") && line.endsWith("00"))
									line = line.substring(0, line.length() -3);
								else if (line.contains(".") && line.endsWith("0"))
									line = line.substring(0, line.length() -1);
								out.printf("%s", line + "\r\n");
							} else {
								out.printf("%s", "NULL");
								out.printf("%s", "\r\n");
							}
						}
					}
				}
				else
				{
					break;
				}
			}
			logger.info("printToFile Finished");
		}
	}
	
	public static void generateReport(String sourcePath, String targetPath) {
		// find the difference and store in a file
		try {
			String filename = "C:\\demo\\filediff_" + new SimpleDateFormat("yyyy-MM-dd.HH.mm'.txt'").format(new Date());
			//Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", "diff " +hanaPath + " " + hivePath + " >" + filename});
			Runtime.getRuntime().exec(new String[] { "cmd", "/k", "fc" + " " + sourcePath + " " + targetPath + " >" + filename});
			System.out.println("source and target are different. Please check the file filediff.txt for mismatched records");
			logger.info("source and target are different. Please check the file filediff.txt for mismatched records");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
}
