package org.aksw.sparqlmap;

import java.io.InputStreamReader;

import org.aksw.sparqlmap.config.syntax.SparqlMapConfiguration;
import org.aksw.sparqlmap.config.syntax.SimpleConfigParser;
import org.aksw.sparqlmap.db.SQLAccessFacade;
import org.junit.After;
import org.junit.Before;

public abstract class BaseTest {
	
	protected SQLAccessFacade db = null;
	
	protected SparqlMapConfiguration config;
	

	
	
	@Before
	public void initMapper() throws Throwable {

		SimpleConfigParser parser = new SimpleConfigParser();

		config = parser.parse(new InputStreamReader(
				ClassLoader.getSystemResourceAsStream("bsbm.r2rml")));



		db = new SQLAccessFacade(config.getDbConn());



	}
	
	@After
	public void close(){
		db.close();
		
	}
	
	
	
	
	abstract public String processQuery(String shortname, String query);

}
