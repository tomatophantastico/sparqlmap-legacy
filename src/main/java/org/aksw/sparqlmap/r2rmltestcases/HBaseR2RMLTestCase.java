package org.aksw.sparqlmap.r2rmltestcases;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.hsqldb.Server;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class HBaseR2RMLTestCase extends R2RMLTest{
	
	

	
	public HBaseR2RMLTestCase(String testCaseName, String r2rmlLocation,
			String outputLocation, String referenceOutput,
			String dbFileLocation, boolean createDM) {
		super(testCaseName, r2rmlLocation, outputLocation, referenceOutput,
				dbFileLocation, createDM);
	}

	



	@Parameters(name="{0}")
	public static Collection<Object[]> data() {
		return data(getTestCaseLocations());
		
	}
	

	Server server = null;
	
	
	@Override
	public void flushDatabase() throws ClassNotFoundException, SQLException {
		//no need to flush anything here.
	
	}
	
	
	@Override
	public void loadFileIntoDB(String file) throws ClassNotFoundException,
			SQLException, IOException {
		
			Connection conn = getConnection();
			SqlFile sqlFile = new SqlFile(new File(file));
			sqlFile.setConnection(conn);
			try {
				sqlFile.execute();
			} catch (SqlToolError e) {				e.printStackTrace();
			}
			
			conn.close();
		
	}
	
	@Before
	public void initServer() {

		server = new Server();
		server.setSilent(true);
		server.setDatabaseName(0, "r2rml" );
		server.setDatabasePath(0, "mem:r2rmldb-" + testCaseName);
		server.start();
	}
	
	@After
	public void shutdownServer(){
		server.shutdown();
	}



	
	public String getDBname() {
		return "hsqldb";
		
	}

	
	public static String getTestCaseLocations() {
		
		return "./testcases/hsqldb/";
	}

	@Override
	public Connection getConnection() throws ClassNotFoundException, SQLException {
		Class.forName("org.hsqldb.jdbcDriver");
       
        Connection connection = DriverManager.getConnection(
            "jdbc:hsqldb:hsql://localhost/r2rml", "sa", "");
		return connection;
	}

	@Override
	public Properties getDBProperties() {
		
		
		Properties dbprops = new Properties();
		
		dbprops.setProperty("jdbc.url","jdbc:hsqldb:hsql://localhost/r2rml");
		 
		dbprops.setProperty("jdbc.username","sa");
		dbprops.setProperty("jdbc.password","");
		dbprops.setProperty("jdbc.poolminconnections","5");
		dbprops.setProperty("jdbc.poolmaxconnections","10");
		return dbprops;
	}

}
