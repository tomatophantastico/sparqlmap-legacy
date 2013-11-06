package org.aksw.sparqlmap.bsbmtestcases;

import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.aksw.sparqlmap.r2rmltestcases.PostgreSQLR2RMLTestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

public class BSBMPostgreSQLTests extends BSBMBaseTest{
	
	private SparqlMap r2r;
	private ApplicationContext con;
	
	static Logger log = LoggerFactory.getLogger(PostgreSQLR2RMLTestCase.class);

	@Before
	public void setupSparqlMap() {
		
		String pathToConf = "./src/main/resources/bsbm-test";
		
		con = ContextSetup.contextFromFolder(pathToConf);
		r2r = (SparqlMap) con.getBean("sparqlMap");

	}
	
	
	
	@Test
	public void testFilterOpt() throws SQLException{
		
		String query = "SELECT ?o {<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product22>  <http://www.w3.org/2000/01/rdf-schema#label> ?o} ";
		String ps = execAsText(query);
		log.info(ps);
		
	}
	

	String  q1 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\n" + 
			"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
			"\n" + 
			"SELECT DISTINCT ?product " + 
			"WHERE { \n" + 
			 "?product rdfs:label ?label .\n" + 
			"    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature439> . \n" + 
			"    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature4> . \n" + 
			"        }\n";
	
	@Test
	public void testMultiPropUse() throws SQLException{

		String ps = execAsText(q1);
		log.info(ps);
		
	}
	
	
	String constructPart = "SELECT * \n" + 
			//"  { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Reviewer83> ?p_sm ?o_sm .}\n" + 
			"WHERE\n" + 
			"  { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Reviewer83> ?p_sm ?o_sm }\n" + 
			""; 

	@Test
	public void testConstructPart() throws SQLException{

		String ps = execAsText(constructPart);
		log.info(ps);
		
	}

	
	




	@Override
	public SparqlMap getSparqlMap() {
		return r2r;
	}

}
