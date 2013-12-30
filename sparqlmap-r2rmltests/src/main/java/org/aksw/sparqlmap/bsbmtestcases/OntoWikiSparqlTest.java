package org.aksw.sparqlmap.bsbmtestcases;

import java.sql.SQLException;

import org.junit.Test;

public class OntoWikiSparqlTest extends BSBMComparingPostgresBaseTest  {
	
	
	@Test
	public void query0() throws SQLException{
		String query = "SELECT  ?0 ?1 \n" + 
				"\n" + 
				"FROM <http://bsbm/10m>\n" + 
				"WHERE { {?s <http://www.w3.org/2000/01/rdf-schema#label> ?0.} UNION {?s <http://purl.org/dc/elements/1.1/title> ?1.} FILTER (sameTerm(?s, <http://bsbm/10m>)) } ";
		
		executeAndCompare(query);
	}
	
	@Test
	public void query1() throws SQLException{
		String query = "SELECT DISTINCT ?resourceUri  \n" + 
				"\n" + 
				"FROM <http://bsbm/10m>\n" + 
				"WHERE { \n" + 
				"{ \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ns.ontowiki.net/SysOnt/Site/Navigation> \n" + 
				"}} \n" + 
				" ";
		executeAndCompare(query);

	}
	
	@Test
	public void query2() throws SQLException{
		String query = "SELECT DISTINCT ?resourceUri  \n" + 
				"\n" + 
				"FROM <http://bsbm/10m>\n" + 
				"WHERE { \n" + 
				"{ \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#AnnotationProperty> \n" + 
				"} \n" + 
				" \n" + 
				"} LIMIT 1\n" + 
				"";
		executeAndCompare(query);

	}
	
	
	@Test
	public void query3() throws SQLException{
		String query = "SELECT DISTINCT ?resourceUri  \n" + 
				"\n" + 
				"FROM <http://bsbm/10m>\n" + 
				"WHERE { \n" + 
				"{ \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ns.aksw.org/spatialHierarchy/SpatialArea> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ns.aksw.org/spatialHierarchy/Planet> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ns.aksw.org/spatialHierarchy/Continent> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ns.aksw.org/spatialHierarchy/Country> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ns.aksw.org/spatialHierarchy/Province> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ns.aksw.org/spatialHierarchy/District> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ns.aksw.org/spatialHierarchy/City> \n" + 
				"} \n" + 
				" \n" + 
				"} LIMIT 1\n" + 
				"";
		executeAndCompare(query);

	}
	
	
	@Test
	public void query4() throws SQLException{
		String query = "SELECT DISTINCT ?resourceUri  \n" + 
				"\n" + 
				"FROM <http://bsbm/10m>\n" + 
				"WHERE { \n" + 
				"{ \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/net/faunistics#Family> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/net/faunistics#Genus> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/net/faunistics#Species> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/net/faunistics#Order> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/net/faunistics#SubOrder> \n" + 
				"} \n" + 
				" \n" + 
				"} LIMIT 1\n" + 
				""; 
		executeAndCompare(query);

	}
	
	
	@Test
	public void query5() throws SQLException{
		String query = "SELECT DISTINCT ?resourceUri  \n" + 
				"\n" + 
				"FROM <http://bsbm/10m>\n" + 
				"WHERE { \n" + 
				"{ \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2004/02/skos/core#Concept> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2004/02/skos/core#ConceptScheme> \n" + 
				"} \n" + 
				" \n" + 
				"} LIMIT 1\n" + 
				"";
		executeAndCompare(query);

	}
	
	@Test
	public void query6() throws SQLException{
		String query = "SELECT DISTINCT ?resourceUri  \n" + 
				"\n" + 
				"FROM <http://bsbm/10m>\n" + 
				"WHERE { \n" + 
				"{ \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Group> \n" + 
				"} \n" + 
				" UNION { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Organization> \n" + 
				"} \n" + 
				" \n" + 
				"} LIMIT 1\n" + 
				"";
		executeAndCompare(query);

	}
	@Test
	public void query7() throws SQLException{
		String query = "SELECT DISTINCT ?resourceUri  \n" + 
				"\n" + 
				"FROM <http://bsbm/10m>\n" + 
				"WHERE { \n" + 
				"{ \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.geneontology.org/dtds/go.dtd#term> \n" + 
				"} \n" + 
				" \n" + 
				"} LIMIT 1\n" + 
				"";
		executeAndCompare(query);

	}
	@Test
	public void query8() throws SQLException{
		String query = "SELECT DISTINCT ?resourceUri  \n" + 
				"\n" + 
				"FROM <http://bsbm/10m>\n" + 
				"WHERE { \n" + 
				"{ \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.mindswap.org/2003/owl/geo/geoFeatures.owl#Country> \n" + 
				"} \n" + 
				" \n" + 
				"} LIMIT 1";
		executeAndCompare(query);

	}
	 
	@Test
	public void query9() throws SQLException{
		String query = "SELECT  ?0 ?1 \n" + 
				"\n" + 
				"			FROM <http://bsbm/10m>\n" + 
				"			WHERE { {?s <http://www.w3.org/2000/01/rdf-schema#label> ?0.} UNION {?s <http://purl.org/dc/elements/1.1/title> ?1.} FILTER (sameTerm(?s, <http://bsbm/10m>)) }";
		executeAndCompare(query);

	}

	@Test
	public void query10() throws SQLException{
		String query = "SELECT  ?0 ?1 \n" + 
				"\n" + 
				"FROM <http://bsbm/10m>\n" + 
				"WHERE { {?s <http://www.w3.org/2000/01/rdf-schema#label> ?0.} UNION {?s <http://purl.org/dc/elements/1.1/title> ?1.} FILTER (sameTerm(?s, <http://bsbm/10m>)) } ";
		executeAndCompare(query);

	}
	@Test
	public void query11() throws SQLException{
		String query = "SELECT DISTINCT ?resourceUri  \n" + 
				"\n" + 
				"FROM <http://bsbm/10m>\n" + 
				"WHERE { \n" + 
				"?resourceUri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?classUri \n" + 
				"OPTIONAL { \n" + 
				"?subResourceUri <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?resourceUri \n" + 
				"}  \n" + 
				"OPTIONAL { \n" + 
				"?resourceUri <http://ns.ontowiki.net/SysOnt/hidden> ?reg \n" + 
				"}  \n" + 
				"OPTIONAL { \n" + 
				"{ \n" + 
				"?resourceUri <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?super \n" + 
				"} \n" + 
				" \n" + 
				"}  \n" + 
				"FILTER (?classUri IN (<http://www.w3.org/2002/07/owl#Class>,<http://www.w3.org/2000/01/rdf-schema#Class>)) \n" + 
				"FILTER (!isBLANK(?resourceUri)) \n" + 
				"FILTER (!BOUND(?reg)) \n" + 
				"FILTER (!REGEX(STR(?resourceUri), \"^http://www.w3.org/1999/02/22-rdf-syntax-ns#\")) \n" + 
				"FILTER (!REGEX(STR(?resourceUri), \"^http://www.w3.org/2000/01/rdf-schema#\")) \n" + 
				"FILTER (!REGEX(STR(?resourceUri), \"^http://www.w3.org/2002/07/owl#\")) \n" + 
				"FILTER (REGEX(STR(?super), \"^http://www.w3.org/2002/07/owl#\") || !BOUND(?super)) \n" + 
				"} LIMIT 11\n" + 
				"";
		executeAndCompare(query);

	}

	
	
	

}
