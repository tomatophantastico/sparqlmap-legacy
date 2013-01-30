package org.aksw.sparqlmap.db.impl;

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

import org.aksw.sparqlmap.db.Connector;
import org.aksw.sparqlmap.mapper.translate.ImplementationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class HSQLDBConnector extends Connector {
	
	
	private BoneCP connectionPool = null;
	
	private static Logger log = LoggerFactory.getLogger(HSQLDBConnector.class);
	
	public HSQLDBConnector(String dbConnectionString, String username, String password, int minConnections, int maxConnections) {

		try {
			Class.forName("org.hsqldb.jdbcDriver");

			// setup the connection pool
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(dbConnectionString); 
			config.setUsername(username); 
			config.setPassword(password);
			config.setMinConnectionsPerPartition(minConnections);
			config.setMaxConnectionsPerPartition(maxConnections);
			config.setPartitionCount(1);
			connectionPool = new BoneCP(config); // setup the connection pool

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
	public List<SelectExpressionItem> getSelectItemsForView(Statement view) {
		log.error("getSelectItemsforViewnotImplemented");
		return null;
	}
	
	
	

	
	@Override
	public Map<String,Integer> getDataTypeForView(Statement viewStatement) {
		throw new ImplementationException("getDataTypeForView not implemented");
		
		
		
	}

	
	@Override
	public void close() {
		connectionPool.close();
		
	}

	
	
	
}
