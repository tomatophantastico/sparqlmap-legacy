package org.aksw.sparqlmap.config.syntax;

import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

import org.aksw.sparqlmap.mapper.subquerymapper.algebra.DataTypeHelper;


public class DBConnectionConfiguration {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DBConnectionConfiguration.class);
	
	public static String POSTGRES = "postgresql";
	public static String MYSQL = "mysql";
	
	private DataTypeHelper dataTypeHelper;
	
		
	private String dbConnString;
	
	private String username;
	
	private String password;

	public DBConnectionConfiguration(String dbConnString, String username,
			String password) {
		super();
		this.dbConnString = dbConnString;
		this.username = username;
		this.password = password;
		String dbname =  getJdbcDBName();
		if(dbname.equals("mysql")){
			dataTypeHelper = new MysqlDataTypeHelper();
		}else if(dbname.equals("postgresql")){
			dataTypeHelper = new PostgresDataTypeHandler();
			
		}else{
			log.error("Unknown Database string " + dbname + " encountered");
		}
		
	}

	public String getDbConnString() {
		return dbConnString;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}
	
	public DataTypeHelper getDataTypeHelper() {
		return dataTypeHelper;
	}
	
	public String getJdbcDBName(){
		return dbConnString.split(":")[1];
	}

	public SelectDeParser getSelectDeParser() {
		if(getJdbcDBName().equals(MYSQL)){
			return new SelectDeParser();
		} else if(getJdbcDBName().equals(POSTGRES)){
			return new PostgresqlSelectDeparser();
			
		}
		log.warn("Selected default deparser");
		return new SelectDeParser();
	}
	public ExpressionDeParser getExpressionDeParser(SelectDeParser selectDeParser, StringBuffer out) {
		if(getJdbcDBName().equals(MYSQL)){
			return new ExpressionDeParser(selectDeParser, out);
		} else if(getJdbcDBName().equals(POSTGRES)){
			return new PostgresqlExpressionDeParser(selectDeParser, out);
			
		}
		log.warn("Selected default expresseiondeparser");
		return new ExpressionDeParser(selectDeParser,out);
	}
	
	
	

}
