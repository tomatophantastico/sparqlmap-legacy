package org.aksw.sparqlmap.r2rmltestcases;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.aksw.sparqlmap.db.Connector;
import org.aksw.sparqlmap.db.impl.MySQLConnector;
import org.aksw.sparqlmap.db.impl.PostgeSQLConnector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;

public class PostgreSQLR2RMLTestCase extends R2RMLTest {
	
	public PostgreSQLR2RMLTestCase(String testCaseName, String r2rmlLocation,
			String outputLocation, String referenceOutput,
			String dbFileLocation, boolean createDM) {
		super(testCaseName, r2rmlLocation, outputLocation, referenceOutput,
				dbFileLocation, createDM);
	}

	PostgeSQLConnector connector;
	
	@Parameters(name="{0}")
	public static Collection<Object[]> data() {
		return data(getTestCaseLocations());
		
	}
	
	public static String getTestCaseLocations() {
		
		return "./testcases/postgres/";
	}
	
	
	@After
	public void cleanup(){
		connector.close(); 
		


	}
	
	@Before
	public void before(){
		connector = new PostgeSQLConnector(getDBProperties().getProperty("jdbc.url"),getDBProperties().getProperty("jdbc.username"),getDBProperties().getProperty("jdbc.password"),1,2);
	}
	

	

	@Override
	public Properties getDBProperties() {
		Properties properties = new Properties();
		try {
			properties.load(ClassLoader.getSystemResourceAsStream("postgres.properties"));
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail("Unable to load properties file");
			
		}
		return properties;
	}

	@Override
	public Connector getConnector() {
		
		return connector;
	}
	


	

}
