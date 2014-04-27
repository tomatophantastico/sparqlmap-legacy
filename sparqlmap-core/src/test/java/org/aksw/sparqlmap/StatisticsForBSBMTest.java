package org.aksw.sparqlmap;

import java.io.File;

import org.junit.Test;

/**
 * 
 * Tests the statistics queries of http://code.google.com/p/void-impl/wiki/SPARQLQueriesForStatistics
 * 
 * on the BSBM dataset.
 * 
 * @author joerg
 *
 */
public class StatisticsForBSBMTest extends BSBMTestOnHSQL{
	
	@Test
	public void totalNumberOfTriplesTest(){
		String query = "SELECT (COUNT(*) AS ?no) { ?s ?p ?o  }";
		executeAndCompareSelect(query, new File("./src/test/resources/bsbm/totalNumberOfTriples.qres"));
		
		
	}
	@Test
	public void totalNumberOfEntitiesTest(){
		String query = "SELECT COUNT(distinct ?s) AS ?no { ?s a []  }";
		executeAndCompareSelect(query, new File("./src/test/resources/bsbm/totalNumberOfEntities.qres"));
		
	}
	@Test
	public void totalNumberOfDistinctURISTest(){
		String query = "SELECT (COUNT(DISTINCT ?s ) AS ?no) { { ?s ?p ?o  } UNION { ?o ?p ?s } FILTER(!isBlank(?s) && !isLiteral(?s)) } ";
	}
	@Test
	public void totalNumberOfDistinctClassesTest(){
		String query = "SELECT COUNT(distinct ?o) AS ?no { ?s rdf:type ?o }";
	}
	@Test
	public void totalNumberofDistinctPredicatesTest(){
		String query = "SELECT count(distinct ?p) { ?s ?p ?o }";
	}
	@Test
	public void totalNumberOdDistinctSubjectNodesTest(){
		String query = "SELECT (COUNT(DISTINCT ?s ) AS ?no) {  ?s ?p ?o   } ";
	}
	@Test
	public void totalNumberOfDistinctObjectNodesTest(){
		String query = "SELECT (COUNT(DISTINCT ?o ) AS ?no) {  ?s ?p ?o  filter(!isLiteral(?o)) } ";
	}
	@Test
	public void listAllClassesTest(){
		String query = "SELECT DISTINCT ?type { ?s a ?type }";
	}
	@Test
	public void listAllPredicatesTest(){
		String query = "SELECT DISTINCT ?p { ?s ?p ?o }";
	}
	@Test
	public void instancesPerClassTest(){
		String query = "SELECT  ?class (COUNT(?s) AS ?count ) { ?s a ?class } GROUP BY ?class ORDER BY ?count";
	}
	@Test
	public void triplesPerPredicate(){
		String query = "SELECT  ?p (COUNT(?s) AS ?count ) { ?s ?p ?o } GROUP BY ?p ORDER BY ?count";
	}
	@Test
	public void distinctSubjectsPerPredicateTest(){
		String query = "SELECT  ?p (COUNT(DISTINCT ?s ) AS ?count ) { ?s ?p ?o } GROUP BY ?p ORDER BY ?count";
	}
	@Test
	public void distinctObjectsPerPredicateTest(){
		String query = "SELECT  ?p (COUNT(DISTINCT ?o ) AS ?count ) { ?s ?p ?o } GROUP BY ?p ORDER BY ?count";
	}
	
	
	
	@Override
	public String getTestName() {
		
		return "Dataset_Statistics_on_BSBM";
	}
	
	
	
	

}
