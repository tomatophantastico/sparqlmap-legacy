package org.aksw.sparqlmap;

import java.sql.SQLException;

import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class GraphTest extends HSQLBaseTest{

	@Override
	public String getMappingFile() {
		return "./src/test/resources/graph-test/r2rmltc0009b.ttl";
	}

	@Override
	public String getSQLFile() {
		return "./src/test/resources/graph-test/r2rmltc0009.sql";
	}

	@Override
	public String getTestName() {
		return "graphtest";
	}
	
	
	
	
	
	@Test
	public void getAllFromStudentGraph() throws SQLException{
		String query = "CONSTRUCT {?s ?p ?o} {GRAPH <http://example.com/graph/students> {?s ?p ?o}} ";
		Model expectedResult = ModelFactory.createDefaultModel();
		expectedResult.read("./src/test/resources/graph-test/mappedb.nt");
		executeAndCompareConstruct(query, expectedResult);
		
		
	}

}
