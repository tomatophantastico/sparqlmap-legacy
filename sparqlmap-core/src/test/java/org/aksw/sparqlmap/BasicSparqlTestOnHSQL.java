package org.aksw.sparqlmap;

import java.io.File;

import org.junit.Test;

public class BasicSparqlTestOnHSQL extends BSBMTestOnHSQL {
	
	String prefixes = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> \n PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> \n PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  \n PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n";

	@Override
	public String getTestName() {

		return "Basic SPARQL functionalities";
	}

	@Test
	public void simplePredicateWithMultipleBindingsTest() {
		String query = "SELECT ?o WHERE {  ?s <http://www.w3.org/2000/01/rdf-schema#label> ?o }  order by ?o limit 10";
		executeAndCompareSelect(query, new File(
				"./src/test/resources/bsbm/somelabels.qres"));

	}
	
	@Test
	public void simpleJoin(){
		
		String query = prefixes 
				+ "SELECT ?product ?label ?value1 {?product rdfs:label ?label .?product bsbm:productPropertyNumeric1 ?value1 } order by ?label limit 10";
		executeAndCompareSelect(query, new File(
				"./src/test/resources/bsbm/simpleJoin.qres"));
	}

}
