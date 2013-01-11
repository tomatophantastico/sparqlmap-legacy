package org.aksw.sparqlmap.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.aksw.sparqlmap.mapper.translate.ImplementationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class PostgesqlConnector implements Connector {
	
	
	private BoneCP connectionPool = null;
	
	private static Logger log = LoggerFactory.getLogger(MySQLConnector.class);
	
	private IDBAccess dbconf;
	
	public PostgesqlConnector(String dbConnectionString, String username, String password, int minConnections, int maxConnections) {
		
		

 
		try {
			Class.forName("org.postgresql.Driver");

			// setup the connection pool
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(dbConnectionString); // jdbc url specific to your database, eg jdbc:mysql://127.0.0.1/yourdb
			config.setUsername(username); 
			config.setPassword(password);
			config.setMinConnectionsPerPartition(minConnections);
			config.setMaxConnectionsPerPartition(maxConnections);
			config.setPartitionCount(1);
			connectionPool = new BoneCP(config); // setup the connection pool
			
		
		
			//setupDriver(dbconf.getDbConnString(),dbconf.getUsername(),dbconf.getPassword());
		} catch (Exception e) {
			log.error("Error setting up the db pool",e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.aksw.r2rj.db.Connector#getConnection()
	 */
	public Connection getConnection() throws SQLException{
	

			return connectionPool.getConnection();
	
	}

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
	public List<SelectExpressionItem> getSelectItemsForView(Statement view) {
		log.error("getSelectItemsforViewnotImplemented");
		return null;
	}
	
	
	

	
	@Override
	public Map<String,Integer> getDataTypeForView(Statement viewStatement) {
		throw new ImplementationException("getDataTypeForView not implemented");
		
		
		
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
	public void close() {
		connectionPool.close();
		
	}

	
	
	
}
