package org.aksw.sparqlmap.db.impl;

import java.util.Properties;

import org.aksw.sparqlmap.db.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HSQLDBConnector extends Connector {
	

	public HSQLDBConnector(String dbUrl, String username, String password,
			Integer poolminconnections, Integer poolmaxconnections) {
		super(dbUrl, username, password, poolminconnections, poolmaxconnections);
	}





	private static Logger log = LoggerFactory.getLogger(HSQLDBConnector.class);
	
	
	


	@Override
	public String getJDBCDriverClassString() {
		
		return "org.hsqldb.jdbcDriver";
	}

	
	
	
}
