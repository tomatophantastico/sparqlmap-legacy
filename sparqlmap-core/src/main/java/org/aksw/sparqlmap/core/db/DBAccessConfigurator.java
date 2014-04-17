package org.aksw.sparqlmap.core.db;

import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.PostConstruct;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.db.impl.HSQLDBConnector;
import org.aksw.sparqlmap.core.db.impl.HSQLDBDataTypeHelper;
import org.aksw.sparqlmap.core.db.impl.MySQLConnector;
import org.aksw.sparqlmap.core.db.impl.MySQLDataTypeHelper;
import org.aksw.sparqlmap.core.db.impl.PostgeSQLConnector;
import org.aksw.sparqlmap.core.db.impl.PostgreSQLDataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;

@Component
public class DBAccessConfigurator {
	
	
	@Autowired
	private Environment env;
	
	
	private String dbname;
	
	private BoneCPDataSource bcp;
	
	
	@PostConstruct
	public void setUpConnection() throws SQLException{
		
			
		String dbUrl = env.getProperty("jdbc.url");
		String username = env.getProperty("jdbc.username");
		String password = env.getProperty("jdbc.password");
		Integer poolminconnections = env.getProperty("jdbc.poolminconnections")!=null?Integer.parseInt(env.getProperty("jdbc.poolminconnections")):null;
		Integer poolmaxconnections = env.getProperty("jdbc.poolmaxconnections")!=null?Integer.parseInt(env.getProperty("jdbc.poolmaxconnections")):null;
		
		
		BoneCPConfig config = createConfig(dbUrl, username, password,
				poolminconnections, poolmaxconnections);
		bcp  = new BoneCPDataSource(config);
		
		Connection conn = bcp.getConnection();
		dbname = conn.getMetaData().getDatabaseProductName();
		conn.close();
		
						
	}
	
	
	


	public static BoneCPConfig createConfig(String dbUrl, String username,
			String password, Integer poolminconnections,
			Integer poolmaxconnections) {
		if (poolmaxconnections==null){
			poolmaxconnections =10;
		}
		if (poolminconnections == null){
			poolminconnections = 5;
		}
		
		
		BoneCPConfig config = new BoneCPConfig();
		config.setJdbcUrl(dbUrl); 
		config.setUsername(username); 
		config.setPassword(password);
		config.setMinConnectionsPerPartition(poolminconnections);
		config.setMaxConnectionsPerPartition(poolmaxconnections);
		config.setPartitionCount(1);
		return config;
	}
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DBAccessConfigurator.class);
	
	
	public  DBAccessConfigurator() {
	}
	
//    public DBAccessConfigurator(File databaseConfFileName) throws FileNotFoundException, IOException {
//		
//		Properties props = new Properties();
//		props.load(new FileInputStream(databaseConfFileName));
//		init(props);
//		
//	}
//	public DBAccessConfigurator(Properties props){
//		
//		init(props);
//		
//	}

	@Bean
	public DataTypeHelper getDataTypeHelper() {
		
		
		if(dbname.equals(HSQLDBDataTypeHelper.getDBName())){
			return new HSQLDBDataTypeHelper();
		}else if(dbname.equals(MySQLDataTypeHelper.getDBName())){
			return new MySQLDataTypeHelper();
		}else if(dbname.equals(PostgreSQLDataTypeHelper.getDBName())){
			return new PostgreSQLDataTypeHelper();
		}
		
		
		throw new ImplementationException("Unknown Database " + dbname + " encountered");

	}
	
	@Bean
	public DBAccess getDBAccess(){
		
		if(dbname.equals(PostgeSQLConnector.POSTGRES_DBNAME)){
				PostgeSQLConnector conn = new PostgeSQLConnector();
				conn.setDs(bcp);
				return new DBAccess(conn);
		}else if(dbname.equals(MySQLConnector.MYSQL_DBNAME)){
			MySQLConnector conn = new MySQLConnector();
			conn.setDs(bcp);
			return new DBAccess(conn);
		}else if(dbname.equals(HSQLDBConnector.HSQLDB_NAME)){
			HSQLDBConnector conn = new HSQLDBConnector();
			conn.setDs(bcp);
			return new DBAccess(conn);
		}
		
		throw new ImplementationException("Unknown Database " + dbname + " encountered");
	}
	
	
	
	
	
	public String getDBName(){
		return dbname;
	}
	
	
//	private void init(Properties props) {
//		this.dbUrl = props.getProperty("jdbc.url");
//		this.username = props.getProperty("jdbc.username");
//		this.password = props.getProperty("jdbc.password");
//		this.poolminconnections = props.getProperty("jdbc.poolminconnections");
//		this.poolmaxconnections = props.getProperty("jdbc.poolmaxconnections");
//	}
//	
	
	
	

}
