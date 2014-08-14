package org.aksw.sparqlmap;

import java.io.File;

import org.junit.Test;

public class Sparql11TestOnHSQL extends BSBMTestOnHSQL{
	
	@Test
	public void bindTest(){
		String query = "SELECT distinct ?x { ?s <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual1>  ?o."
				+ "					BIND (\"somestring\"@en as ?x) "
				+ "FILTER (?x != ?s ) } ORDER BY ?s limit 10";
		executeAndCompareSelect(query, new File("./src/test/resources/sparql11/totalNumberOfTriples.qres"));
		
		
	}
	

	
	@Test
	public void selectPersonsTest(){
		String query = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT ?s WHERE {  ?s a foaf:Person } LIMIT 10";
		executeAndCompareSelect(query, new File("./src/test/resources/bsbm/somepersons.qres"));
		
		
	}
	
	
	

	@Override
	public String getTestName() {
		
		return "SPARQL11_Tests";
	}
}
