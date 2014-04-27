package org.aksw.sparqlmap;
/**
 * This is the base test for querying BSBM.
 * 
 * Loads the database and the mapping.
 * 
 * @author joerg
 *
 */
public abstract class BSBMTestOnHSQL extends HSQLBaseTest{

	@Override
	public String getMappingFile() {
		
		return "./src/test/resources/bsbm/bsbm-r2rml.ttl";
	}

	@Override
	public String getSQLFile() {
		
		return "./src/test/resources/bsbm/bsbm2-100k-hsql.sql";
	}

	

}
