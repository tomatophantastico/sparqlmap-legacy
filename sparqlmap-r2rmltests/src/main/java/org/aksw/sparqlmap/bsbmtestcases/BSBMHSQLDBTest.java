package org.aksw.sparqlmap.bsbmtestcases;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.db.Connector;
import org.aksw.sparqlmap.core.db.DBAccessConfigurator;
import org.aksw.sparqlmap.core.db.impl.HSQLDBConnector;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.hsqldb.Server;
import org.hsqldb.cmdline.SqlFile;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.jolbox.bonecp.BoneCPDataSource;

public class BSBMHSQLDBTest extends BSBMBaseTest{
	private static Logger log = LoggerFactory.getLogger(BSBMHSQLDBTest.class);
	public static String hsqldbFileLocation = "./target/hsqldbfiles/db";
	private Server server;
	private SparqlMap r2r;
	private ApplicationContext con;

	@Before
	public void init() {
		initDatabase();
		setupSparqlMap();

	}

	@After
	public void close() {
//		try {
//			Thread.sleep(3600000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		server.shutdown();
	}

	public void setupSparqlMap() {
		
		
		Properties props = getDBProperties();
		
		
		
		Map<String,Properties> name2props = new HashMap<String,Properties>();
		name2props.put("hsql-conf", props);
		
		con = ContextSetup.contextFromProperties(name2props);
		r2r = (SparqlMap) con.getBean("sparqlMap");

	}

	public void initDatabase() {

		server = new Server();
		server.setSilent(true);
		server.setDatabaseName(0, "bsbm2-100k");
		server.setDatabasePath(0, "file:" + hsqldbFileLocation);

		File hsqlFolder = new File(hsqldbFileLocation +".tmp");
		if (hsqlFolder.exists()) {
			server.start();
		} else {
			server.start();
			Connection conn = null;
			try {
				conn = getConnector().getConnection();
				SqlFile schemaSqlFile = new SqlFile(new File(
						"./src/main/resources/bsbm-datasets/bsbm2-schema-hsql.sql"));
				schemaSqlFile.setConnection(conn);
				schemaSqlFile.execute();
				conn.commit();
				SqlFile dataSqlFile = new SqlFile(new File(
						"./src/main/resources/bsbm-datasets/bsbm2-data-100k.sql"));
				dataSqlFile.setConnection(conn);
				dataSqlFile.execute();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if (conn != null) {
						conn.close();
					}

				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

	}

	public Connector getConnector() {
		BoneCPDataSource ds = new BoneCPDataSource(
				DBAccessConfigurator.createConfig(
						getDBProperties().getProperty("jdbc.url"), getDBProperties().getProperty("jdbc.username"), getDBProperties().getProperty("jdbc.password"), 1, 2));
		
		
		HSQLDBConnector conn = new HSQLDBConnector();
		conn.setDs(ds);
		return conn;

	}

	public Properties getDBProperties() {
		Properties props = new Properties();
		
		props.put("sm.mappingfile", "./src/main/resources/bsbm-test/bsbm-r2rml.ttl");

		// replicating the values from initDatabase
		props.put("jdbc.url","jdbc:hsqldb:file:" + hsqldbFileLocation);
		props.put("jdbc.username","sa");
		props.put("jdbc.password","");
		
		return props;
	}
	
	
	
	

	
	
	

	@Override
	public SparqlMap getSparqlMap() {
		return r2r;
	}
	
	
	
	
	
	
	
	
	
}
