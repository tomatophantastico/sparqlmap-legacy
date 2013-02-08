package org.aksw.sparqlmap.db.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.aksw.sparqlmap.db.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPConfig;

public class MySQLConnector extends Connector {
	


	public MySQLConnector(String dbUrl, String username, String password,
			Integer poolminconnections, Integer poolmaxconnections) {
		super(dbUrl, username, password, poolminconnections, poolmaxconnections);
	}

	private static Logger log = LoggerFactory.getLogger(MySQLConnector.class);
	



	@Override
	public List<SelectExpressionItem> getSelectItemsForTable(Table table) {
		Connection conn = null;
		List<SelectExpressionItem> items = new ArrayList<SelectExpressionItem>();
		try {
			conn = getConnection();
			java.sql.Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("describe " + table+";");
			while(rs.next()){
				SelectExpressionItem item = new SelectExpressionItem();
				item.setExpression(new Column(table, rs.getString("Field")));
				items.add(item);	
			}
		} catch (SQLException e) {
			log.error("Error querying table structure", e);
		}finally{
			try {
				if(conn!=null){
					conn.close();
				}
			} catch (SQLException e) {
				log.error("Error:",e);
			}
		}
		
		return items;
	}

	
	

	
	

	@Override
	public Map<String,Integer> getDataTypeForTable(Table table) {
		
		Map<String,Integer> returnTypes = new HashMap<String, Integer>();
		Connection conn = null;
		try {
			conn = getConnection();
			java.sql.Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from  " + table+" limit 1");
			for(int i = 1; i<= rs.getMetaData().getColumnCount(); i++){
				returnTypes.put(rs.getMetaData().getColumnLabel(i), rs.getMetaData().getColumnType(i));
			}
		} catch (SQLException e) {
			log.error("Error querying table structure", e);
		}finally{
			try {
				if(conn!=null){
				conn.close();
				}
			} catch (SQLException e1) {
				log.error("Error:",e1);
			}
		}
		
		return returnTypes;
	}
	
	@Override
	public String getJDBCDriverClassString() {
		
		return "com.mysql.jdbc.Driver";
	}
	
	@Override
	public BoneCPConfig getBoneCPConfig(String dbUrl, String username, String password,
			Integer poolminconnections, Integer poolmaxconnections) {
		BoneCPConfig conf = super.getBoneCPConfig(dbUrl, username, password, poolminconnections, poolmaxconnections);
		String dbConnectionString = conf.getJdbcUrl();
		
		if(!dbConnectionString.contains("?")){
			dbConnectionString += "?";
		}else{
			dbConnectionString +=  "&";
		}
		
		if(!dbConnectionString.contains("padCharsWithSpace")){
			dbConnectionString +=  "padCharsWithSpace=true";
		}
		if(dbConnectionString.contains("sessionVariables")){
			log.warn("Session variables contained in url string. make sure it sets the ");
		}else{
			dbConnectionString += "&sessionVariables=sql_mode='ANSI_QUOTES'";
		}
		
		conf.setJdbcUrl(dbConnectionString);
		
		return conf;
	}

	
	

}
