package org.aksw.sparqlmap.core.db.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.aksw.sparqlmap.core.db.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


public class PostgeSQLConnector extends Connector {
	
	
	public static final String POSTGRES_DBNAME = "PostgreSQL";


	{
		try{
			Class.forName("org.postgresql.Driver" ); 
		} catch (Exception e) {
			LoggerFactory.getLogger(MySQLConnector.class).info("PostgreSQL driver not present",e);
		}
			
		
	}

	private static Logger log = LoggerFactory.getLogger(MySQLConnector.class);
	

	@Override
	public List<SelectExpressionItem> getSelectItemsForTable(Table table) {
		Connection conn = null;
		List<SelectExpressionItem> items = new ArrayList<SelectExpressionItem>();
		try {
			conn = getConnection();
			java.sql.Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT column_name FROM information_schema.columns WHERE table_name ='" + table+"' ORDER BY ordinal_position;");
			while(rs.next()){
				SelectExpressionItem item = new SelectExpressionItem();
				item.setExpression(new Column(table, rs.getString("column_name")));
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
			} catch (SQLException e) {
				log.error("Error:",e);
			}
		}
		
		return returnTypes;
	}
	

@Override
public String getDBName() {
	return POSTGRES_DBNAME;
}
	
	
}
