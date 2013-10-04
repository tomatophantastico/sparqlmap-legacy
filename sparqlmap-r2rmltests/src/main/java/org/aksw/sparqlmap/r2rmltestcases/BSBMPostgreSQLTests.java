package org.aksw.sparqlmap.r2rmltestcases;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.aksw.sparqlmap.SparqlMap;
import org.aksw.sparqlmap.SparqlMap.ReturnType;
import org.aksw.sparqlmap.db.SQLResultSetWrapper;
import org.aksw.sparqlmap.spring.ContextSetup;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.hp.hpl.jena.query.QueryFactory;

public class BSBMPostgreSQLTests extends BSBMBaseTest{
	
	private SparqlMap r2r;
	private ApplicationContext con;
	
	static Logger log = LoggerFactory.getLogger(PostgreSQLR2RMLTestCase.class);

	@Before
	public void setupSparqlMap() {
		
		
		String pathToConf = "./src/main/resources/bsbmpostgresql";
		
		con = ContextSetup.contextFromFolder(pathToConf);
		r2r = (SparqlMap) con.getBean("sparqlMap");

	}
	
	
	
	@Test
	public void testFilterOpt() throws SQLException{
		
		String query = "SELECT DISTINCT ?o {<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product22>  <http://www.w3.org/2000/01/rdf-schema#label> ?o} limit 5";
		String ps = execAsText(query);
		log.info(ps);
		
	}

	
	




	@Override
	public SparqlMap getSparqlMap() {
		return r2r;
	}

}
