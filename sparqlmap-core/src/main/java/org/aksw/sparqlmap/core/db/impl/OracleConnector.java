package org.aksw.sparqlmap.core.db.impl;

import java.util.Properties;

import org.aksw.sparqlmap.core.db.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleConnector extends Connector {
	
	
	
	



	public OracleConnector(String dbUrl, String username, String password,
			Integer poolminconnections, Integer poolmaxconnections) {
		super(dbUrl, username, password, poolminconnections, poolmaxconnections);
	}





	private static Logger log = LoggerFactory.getLogger(OracleConnector.class);
	
	
	


	@Override
	public String getJDBCDriverClassString() {
		
		return "org.hsqldb.jdbcDriver";
	}

	
	
	
}
