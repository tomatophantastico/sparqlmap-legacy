package org.aksw.sparqlmap;

import org.aksw.sparqlmap.db.DBAccess;
import org.aksw.sparqlmap.spring.ContextSetup;
import org.junit.Before;
import org.springframework.context.ApplicationContext;

public abstract class BSBMBaseTest {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(BSBMBaseTest.class);
	
	public SparqlMap r2r;

	private DBAccess dbConf;

	public ApplicationContext con;
	
	

	

	@Before
	public void setUp() throws Exception {
		String pathToConf = "./src/test/conf/bsbm";
		
		con = ContextSetup.contextFromFolder(pathToConf);
		r2r = (SparqlMap) con.getBean("sparqlMap");
		
		
		

	}
	
	
	
	
	abstract public String processQuery(String shortname, String query);

}
