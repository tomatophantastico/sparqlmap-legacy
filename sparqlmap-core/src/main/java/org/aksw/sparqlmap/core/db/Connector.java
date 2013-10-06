package org.aksw.sparqlmap.core.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
/**
 * holds the connection pool and some other interesting stuff
 * @author joerg
 *
 */
public abstract class Connector {
	
	private static Logger log = LoggerFactory.getLogger(Connector.class);

	public Connector( ){
	}
	
	
	
	
	public void setDs(BoneCPDataSource ds){
		this.connectionPool =ds; 
	}
	

	public Connection getConnection() throws SQLException{
		return connectionPool.getConnection(); 
	}
	
	
	public JdbcTemplate getTemplate(){
		return new JdbcTemplate(connectionPool);
	}
	
	public DataSource getDataSource(){
		return connectionPool;
	}
	
	
	protected BoneCPDataSource connectionPool = null;
	
	
	
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
	
	public abstract String getDBName();


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


	public void close(){
		connectionPool.close();
	}


}