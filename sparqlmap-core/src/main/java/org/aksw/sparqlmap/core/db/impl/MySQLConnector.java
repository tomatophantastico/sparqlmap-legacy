package org.aksw.sparqlmap.core.db.impl;

import javax.annotation.PostConstruct;

import org.aksw.sparqlmap.core.MappingException;
import org.aksw.sparqlmap.core.db.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.jolbox.bonecp.BoneCPDataSource;

public class MySQLConnector extends Connector {
	
	public static final String MYSQL_DBNAME = "MySQL";
	private static Logger log = LoggerFactory.getLogger(MySQLConnector.class);
	
	
	{
		try{
			Class.forName("com.mysql.jdbc.Driver" ); 
		} catch (Exception e) {
			LoggerFactory.getLogger(MySQLConnector.class).info("MySQL driver not present",e);
		}
			
		
	}


	@PostConstruct
	public void validateDB(){
		
	}
	
	@Override
	@Autowired
	public void setDs(BoneCPDataSource ds) {
		String dbConnectionString = ds.getConfig().getJdbcUrl();
		
		if(!dbConnectionString.contains("padCharsWithSpace")){
			throw new MappingException("MYSQL requires padCharsWithSpace=true to be set in the jdbc url");
		}
		if(!dbConnectionString.contains("ANSI_QUOTES")){
			throw new MappingException("MYSQL requires sessionVariables=sql_mode='ANSI_QUOTES' to be set in the jdbc url");
		
		}
		this.connectionPool  = ds;
	}
	
	
	@Override
	public String getDBName() {
		return MYSQL_DBNAME;
	}
}
		
	
	



//	@Override
//	public List<SelectExpressionItem> getSelectItemsForTable(Table table) {
//		Connection conn = null;
//		List<SelectExpressionItem> items = new ArrayList<SelectExpressionItem>();
//		try {
//			conn = getConnection();
//			java.sql.Statement stmt = conn.createStatement();
//			ResultSet rs = stmt.executeQuery("describe " + table+";");
//			while(rs.next()){
//				SelectExpressionItem item = new SelectExpressionItem();
//				item.setExpression(new Column(table, rs.getString("Field")));
//				items.add(item);	
//			}
//		} catch (SQLException e) {
//			log.error("Error querying table structure", e);
//		}finally{
//			try {
//				if(conn!=null){
//					conn.close();
//				}
//			} catch (SQLException e) {
//				log.error("Error:",e);
//			}
//		}
//		
//		return items;
//	}

//	
//	
//
//	
//	
//
//	@Override
//	public Map<String,Integer> getDataTypeForTable(Table table) {
//		
//		Map<String,Integer> returnTypes = new HashMap<String, Integer>();
//		Connection conn = null;
//		try {
//			conn = getConnection();
//			java.sql.Statement stmt = conn.createStatement();
//			ResultSet rs = stmt.executeQuery("select * from  " + table+" limit 1");
//			for(int i = 1; i<= rs.getMetaData().getColumnCount(); i++){
//				returnTypes.put(rs.getMetaData().getColumnLabel(i), rs.getMetaData().getColumnType(i));
//			}
//		} catch (SQLException e) {
//			log.error("Error querying table structure", e);
//		}finally{
//			try {
//				if(conn!=null){
//				conn.close();
//				}
//			} catch (SQLException e1) {
//				log.error("Error:",e1);
//			}
//		}
//		
//		return returnTypes;
//	}
//	
//
//	
//	@Override
//	public BoneCPConfig getBoneCPConfig(String dbUrl, String username, String password,
//			Integer poolminconnections, Integer poolmaxconnections) {
//		BoneCPConfig conf = super.getBoneCPConfig(dbUrl, username, password, poolminconnections, poolmaxconnections);
//		String dbConnectionString = conf.getJdbcUrl();
//		
//		if(!dbConnectionString.contains("?")){
//			dbConnectionString += "?";
//		}else{
//			dbConnectionString +=  "&";
//		}
//		
//		
//		
//		conf.setJdbcUrl(dbConnectionString);
//		
//		return conf;
//	}

	
