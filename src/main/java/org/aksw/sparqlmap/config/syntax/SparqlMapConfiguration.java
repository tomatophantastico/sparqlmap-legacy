package org.aksw.sparqlmap.config.syntax;

import org.aksw.sparqlmap.config.syntax.r2rml.R2RMLModel;
import org.aksw.sparqlmap.db.DBAccess;

public class SparqlMapConfiguration {

	private String endpoint;

	private R2RMLModel mappingConfiguration;

	private DBAccess dbConn;
	
	private boolean useSolutionModifierPushing = true;
	
	public boolean isUseSolutionModifierPushing() {
		return useSolutionModifierPushing;
	}
	
	public void setUseSolutionModifierPushing(boolean useSolutionModifierPushing) {
		this.useSolutionModifierPushing = useSolutionModifierPushing;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public DBAccess getDbConn() {
		return dbConn;
	}

	public void setDbConn(DBAccess dbConn) {
		this.dbConn = dbConn;
	}




}
