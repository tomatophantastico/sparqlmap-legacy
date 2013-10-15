package org.aksw.sparqlmap.bsbmtestcases;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
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
		
		
		String pathToConf = "./src/main/resources/bsbmhsql";
		
		con = ContextSetup.contextFromFolder(pathToConf);
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
						"./src/main/resources/bsbm/bsbm2-schema-hsql.sql"));
				schemaSqlFile.setConnection(conn);
				schemaSqlFile.execute();
				conn.commit();
				SqlFile dataSqlFile = new SqlFile(new File(
						"./src/main/resources/bsbm/bsbm2-data-100k.sql"));
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

		Properties dbprops = new Properties();

		try {
			dbprops.load(new FileInputStream("./src/main/resources/bsbmhsql/db.properties"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dbprops;
	}
	
	
	
	

	
	
	

	@Override
	public SparqlMap getSparqlMap() {
		return r2r;
	}
	
	
	
	
	
	
	
	
	
}
