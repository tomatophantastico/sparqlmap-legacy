package org.aksw.sparqlmap.r2rmltestcases;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import org.aksw.sparqlmap.core.db.Connector;
import org.aksw.sparqlmap.core.db.DBAccessConfigurator;
import org.aksw.sparqlmap.core.db.impl.HSQLDBConnector;
import org.hsqldb.Server;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;

import com.jolbox.bonecp.BoneCPDataSource;

public class HSQLR2RMLTestCase extends R2RMLTest{
	
	

	
	public HSQLR2RMLTestCase(String testCaseName, String r2rmlLocation,
			String outputLocation, String referenceOutput,
			String dbFileLocation, boolean createDM) {
		super(testCaseName, r2rmlLocation, outputLocation, referenceOutput,
				dbFileLocation, createDM);
	}

	



	@Parameters(name="{0}")
	public static Collection<Object[]> data() {
		return data(getTestCaseLocations());
		
	}
	
	public static String getTestCaseLocations() {
		
		return "./testcases/hsqldb/";
	}
	

	Server server = null;
	
	
	@Override
	public void flushDatabase() throws ClassNotFoundException, SQLException {
		//no need to flush anything here.
	
	}
	
	
	@Override
	public void loadFileIntoDB(String file) throws ClassNotFoundException,
			SQLException, IOException {
		
			Connection conn = getConnector().getConnection();
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



	@Override
	public Connector getConnector(){
		
		BoneCPDataSource ds = new BoneCPDataSource(DBAccessConfigurator.createConfig(
				getDBProperties().getProperty("jdbc.url"), 
				getDBProperties().getProperty("jdbc.username"), 
				getDBProperties().getProperty("jdbc.password"), 1, 2));
		
		HSQLDBConnector conn  = new HSQLDBConnector();
		
		conn.setDs(ds);
		return conn;
		
	}

	@Override
	public Properties getDBProperties() {
		
		Properties properties = new Properties();
		try {
			properties.load(ClassLoader.getSystemResourceAsStream("r2rml-test/db-hsql.properties"));
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail("Unable to load properties file");
			
		}
		return properties;
	}

}
