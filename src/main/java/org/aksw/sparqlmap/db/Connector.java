package org.aksw.sparqlmap.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public abstract class Connector {
	
	private static Logger log = LoggerFactory.getLogger(Connector.class);


	public abstract Connection getConnection() throws SQLException;
	
	
	public  abstract List<SelectExpressionItem> getSelectItemsForView(Statement view);



	public List<SelectExpressionItem> getSelectItemsForTable(Table table){
		List<SelectExpressionItem> items = new ArrayList<SelectExpressionItem>();
		Connection conn = null;

		try {
			conn = getConnection();

			DatabaseMetaData metadata = conn.getMetaData();
			ResultSet resultSet = metadata.getColumns(null, null,
					table.getName(), null);
			while (resultSet.next()) {
				String name = resultSet.getString("COLUMN_NAME");

				SelectExpressionItem item = new SelectExpressionItem();
				item.setExpression(new Column(table, name));
				items.add(item);

			}
		} catch (SQLException e) {
			log.error("Error querying table structure", e);
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				log.error("Error:", e);
			}
		}

		return items;
	}
	
	


	public abstract Map<String,Integer> getDataTypeForView(Statement viewStatement);



	public Map<String,Integer> getDataTypeForTable(Table table){
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


	public abstract void close();


}