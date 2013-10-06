package org.aksw.sparqlmap.core.db.impl;

import java.util.Properties;

import org.aksw.sparqlmap.core.db.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.jolbox.bonecp.BoneCPDataSource;

@Component
public class OracleConnector extends Connector {
	

	public static final String ORACLE_DBNAME = "oracle";



	private static Logger log = LoggerFactory.getLogger(OracleConnector.class);
	
	@Override
	public String getDBName() {
		
		return ORACLE_DBNAME;
	}
	


}
