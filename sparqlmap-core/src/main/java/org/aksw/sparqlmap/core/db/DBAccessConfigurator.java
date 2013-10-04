package org.aksw.sparqlmap.core.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.aksw.sparqlmap.core.db.impl.HSQLDBConnector;
import org.aksw.sparqlmap.core.db.impl.HSQLDBDataTypeHelper;
import org.aksw.sparqlmap.core.db.impl.MySQLConnector;
import org.aksw.sparqlmap.core.db.impl.MySQLDataTypeHelper;
import org.aksw.sparqlmap.core.db.impl.OracleConnector;
import org.aksw.sparqlmap.core.db.impl.OracleDataTypeHelper;
import org.aksw.sparqlmap.core.db.impl.PostgeSQLConnector;
import org.aksw.sparqlmap.core.db.impl.PostgreSQLDataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.ImplementationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class DBAccessConfigurator {
	
	private String dbUrl;
	
	private String username;
	
	private String password;
	
	private String poolminconnections;
	
	private String poolmaxconnections;
	
	@Autowired
	private Environment env;
	
	
	@PostConstruct
	public void setprivateValues(){
		dbUrl = env.getProperty("jdbc.url");
		username = env.getProperty("jdbc.username");
		password = env.getProperty("jdbc.password");
		poolminconnections = env.getProperty("jdbc.poolminconnections");
		poolmaxconnections = env.getProperty("jdbc.poolmaxconnections");
	}
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DBAccessConfigurator.class);
	
	
	public  DBAccessConfigurator() {
	}
	
    public DBAccessConfigurator(File databaseConfFileName) throws FileNotFoundException, IOException {
		
		Properties props = new Properties();
		props.load(new FileInputStream(databaseConfFileName));
		init(props);
		
	}
	public DBAccessConfigurator(Properties props){
		
		init(props);
		
	}

	public String getDbConnString() {
		return dbUrl;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}
	
	@Bean
	public DataTypeHelper getDataTypeHelper() {
		String dbname =  getJdbcDBName();
		if(dbname.equals(DBAccess.MYSQL)){
			return new MySQLDataTypeHelper();
		}else if(dbname.equals(DBAccess.POSTGRES) ){
			return new PostgreSQLDataTypeHelper();
		}else if(dbname.equals(DBAccess.HSQLDB)){
			return new HSQLDBDataTypeHelper();
		}else if(dbname.equals(DBAccess.ORACLE)){
			return new OracleDataTypeHelper();
		}		else{
			throw new ImplementationException("Unknown Database string " + dbname + " encountered");
		}
	}
	
	
	
	public String getJdbcDBName(){
		if(env!=null)
		log.info("url is: " + env.getProperty("jdbc.url"));
		return dbUrl.split(":")[1];
	}
	
	
	private void init(Properties props) {
		this.dbUrl = props.getProperty("jdbc.url");
		this.username = props.getProperty("jdbc.username");
		this.password = props.getProperty("jdbc.password");
		this.poolminconnections = props.getProperty("jdbc.poolminconnections");
		this.poolmaxconnections = props.getProperty("jdbc.poolmaxconnections");
	}
	
	
	@Bean
	public DBAccess getDBAccess(){
		log.info("Creating DB Access for: " + dbUrl + "|" + username + ", etc." );
		
		String dbname =  getJdbcDBName();
		Connector dbConnector = null;
		if(dbname.equals(DBAccess.MYSQL)){
			dbConnector = new MySQLConnector(dbUrl, username, password, new Integer(poolminconnections), new Integer(poolmaxconnections));
		}else if(dbname.equals(DBAccess.POSTGRES)){
			dbConnector = new PostgeSQLConnector(dbUrl, username, password, new Integer(poolminconnections), new Integer(poolmaxconnections));
		}else if(dbname.equals(DBAccess.HSQLDB)){
			dbConnector = new HSQLDBConnector(dbUrl, username, password, new Integer(poolminconnections), new Integer(poolmaxconnections));
		}else if(dbname.equals(DBAccess.ORACLE)){
			dbConnector = new OracleConnector(dbUrl, username, password, new Integer(poolminconnections), new Integer(poolmaxconnections));
		}else 
		{
			throw new ImplementationException("Unknown Database string " + dbname + " encountered");
		}
		
		DBAccess access = new DBAccess(dbConnector, dbname);
		return access;
	}
	
	
	

}
