package org.aksw.sparqlmap.bsbmtestcases;

import java.sql.SQLException;

import org.junit.Test;

public class BSBMPostgresComparingTest extends BSBMComparingPostgresBaseTest {
	
	
	@Test
	public void simpleJoin() throws SQLException{
		String sj = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\n" + 
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"\n" + 
				"SELECT * \n" + 
				"WHERE {\n" + 

				"        ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature79> .\n" + 
				"        ?product rdfs:label ?testVar \n" + 
				"}\n" +
				" ORDER by ?testVar \n" +
				"LIMIT 10";
		executeAndCompare(sj);
	}
	
	@Test
	public void simpleLeftJoin() throws SQLException{
		String sj = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\n" + 
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"\n" + 
				"SELECT * \n" + 
				"WHERE {\n" + 

				"        ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature79> .\n" + 
				"        OPTIONAL {?product rdfs:label ?testVar } \n" + 
				"}\n" +
				" ORDER by ?testVar \n" +
				"LIMIT 10";
		executeAndCompare(sj);
	}
	
	
	@Test
	public void testQ05min() throws SQLException{
		
		String q5min = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
				"\n" + 
				"SELECT DISTINCT ?product ?productLabel ?simProperty1 ?origProperty1\n" + 
				"WHERE { \n" + 
				"	?product rdfs:label ?productLabel .\n" + 
				"    FILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer5/Product188> != ?product)\n" + 
				"	<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer5/Product188> bsbm:productFeature ?prodFeature .\n" + 
				"	?product bsbm:productFeature ?prodFeature .\n" + 
				"	<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer5/Product188> bsbm:productPropertyNumeric1 ?origProperty1 .\n" + 
				"	?product bsbm:productPropertyNumeric1 ?simProperty1 .\n" + 
				"	FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120))\n" + 
				"	<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer5/Product188> bsbm:productPropertyNumeric2 ?origProperty2 .\n" + 
				"	?product bsbm:productPropertyNumeric2 ?simProperty2 .\n" + 
				"	FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170))\n" + 
				"}\n" + 
				"ORDER BY ?productLabel\n" + 
				"LIMIT 5"+
				"";
		executeAndCompare(q5min);
	}
	
	@Test
	public void testQ01() throws SQLException{
		executeAndCompare(BSBM100k.q1);
	}
	
	@Test
	public void testQ02() throws SQLException{
		executeAndCompare(BSBM100k.q2);
	}
	
	@Test
	public void testQ03() throws SQLException{
		executeAndCompare(BSBM100k.q3);
	}
	@Test
	public void testQ04() throws SQLException{
		executeAndCompare(BSBM100k.q4);
	}
	@Test
	public void testQ05() throws SQLException{
		executeAndCompare(BSBM100k.q5);
	}
	@Test
	public void testQ07() throws SQLException{
		executeAndCompare(BSBM100k.q7);
	}
	@Test
	public void testQ08() throws SQLException{
		executeAndCompare(BSBM100k.q8);
	}
	@Test
	public void testQ09() throws SQLException{
		executeAndCompare(BSBM100k.q9);
	}
	@Test
	public void testQ10() throws SQLException{
		executeAndCompare(BSBM100k.q10);
	}
	@Test
	public void testQ11() throws SQLException{
		executeAndCompare(BSBM100k.q11);
	}
	@Test
	public void testQ12() throws SQLException{
		executeAndCompare(BSBM100k.q12);
	}

}
