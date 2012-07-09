package org.aksw.sparqlmap;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;

import org.aksw.sparqlmap.RDB2RDF.ReturnType;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.vocabulary.RDF;


public class QueryTests {

	private Logger log = LoggerFactory.getLogger(QueryTests.class);
	RDB2RDF r2r = new RDB2RDF("src/main/conf");

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testQueryByClass() {
		String query = "SELECT * {?s <" +RDF.type.getURI()+"> <http://xmlns.com/foaf/0.1/Person> } LIMIT 100";
		log.info(r2r.mapper.rewrite(QueryFactory.create(query)));

		processQuery("querybytype", query);
	}
	
	
	@Test
	public void testS_P_String() {
		String query = "SELECT * {?s ?p \"Caryn\" } LIMIT 100";
		log.info(r2r.mapper.rewrite(QueryFactory.create(query)));

		processQuery("querybytype", query);
	}
	
	
	
	public String processQuery(String queryShortname, String queryString) {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		try {
			r2r.executeSparql(queryString,ReturnType.XML,result);
		} catch (SQLException e) {
			
			log.error("Error:",e);
		}
		
		String resString =result.toString(); 

		log.info(resString);
		
		return resString;
		
	}

}








