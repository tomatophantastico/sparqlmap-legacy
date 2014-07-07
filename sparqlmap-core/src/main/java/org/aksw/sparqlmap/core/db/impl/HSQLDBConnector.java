package org.aksw.sparqlmap.core.db.impl;

import org.aksw.sparqlmap.core.db.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.jolbox.bonecp.BoneCPDataSource;

public class HSQLDBConnector extends Connector {
	
	{
		try{
			Class.forName("org.hsqldb.jdbcDriver" ); 
		} catch (Exception e) {
			LoggerFactory.getLogger(HSQLDBConnector.class).error("Error loading HSQLDB driver",e);
		}
			
		
	}
	

	public static final String HSQLDB_NAME = "HSQL Database Engine";


	private static Logger log = LoggerFactory.getLogger(HSQLDBConnector.class);
	
	
	@Override
	public String getDBName() {
		return HSQLDB_NAME;
	}

	
	
	
}
