package org.aksw.sparqlmap;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;

import org.aksw.sparqlmap.RDB2RDF;
import org.aksw.sparqlmap.RDB2RDF.ReturnType;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BSBMAlgebraRewriter {
	
	
		
	RDB2RDF r2r = new RDB2RDF("bsbm.r2rml");
	
	@Test
	public void query1Test() {
		
		
		String result = processQuery("q1", BSBMTest.q1);
		
		assertTrue(result.contains("dataFromProducer1092/Product54002"));
		assertTrue(result.contains("gobies tweet counteropening"));
		assertTrue(result.contains("dataFromProducer1905/Product95736"));
		assertTrue(result.contains("despatch"));
		assertTrue(result.contains("dataFromProducer1420/Product70737"));
		assertTrue(result.contains("insects"));
		assertTrue(result.contains("dataFromProducer1304/Product64824"));
		assertTrue(result.contains("torchbearers reenlightens"));
		assertTrue(StringUtils.countMatches(result, "<result>")==4);
		
		
	}
	
	
	@Test
	public void query2Test() {
		
		
		String result = processQuery("q2", BSBMTest.q2);
		
		assertTrue(result.contains("buzzards directories chinking traitress melodies apologetically gendering rethreaded rosins"));
		assertTrue(result.contains("undramatic"));
		assertTrue(StringUtils.countMatches(result, "<result>")==23);
		
		
	}
	

	
	
	
	
	
	
	private Logger log = LoggerFactory.getLogger(BSBMAlgebraRewriter.class);
	
	
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
	
	public void processQuery(String queryShortname, String query,
			String sqlQuery, ArrayList checkKeys) {
		processQuery(queryShortname, query);
	}

}
