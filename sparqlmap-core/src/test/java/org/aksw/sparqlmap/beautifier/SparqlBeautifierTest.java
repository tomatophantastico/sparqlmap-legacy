package org.aksw.sparqlmap.beautifier;

import org.aksw.sparqlmap.core.beautifier.SparqlBeautifier;
import org.junit.Test;

import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpAsQuery;


public class SparqlBeautifierTest {
	
	private SparqlBeautifier beauty = new SparqlBeautifier();
	
	private String q2 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+ "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>  "
			+ "SELECT "
			+ "?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3  ?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4  "
			+ "WHERE {  "
			+ "bsbm-inst:Product1 rdfs:label ?label . 	"
			+ "bsbm-inst:Product1 rdfs:comment ?comment . 	"
			+ "bsbm-inst:Product1 bsbm:producer ?p . 	"
			+ "?p rdfs:label ?producer . "
			+ "bsbm-inst:Product1 dc:publisher ?p .  	"
			+ "bsbm-inst:Product1 bsbm:productFeature ?f . 	"
			+ "?f rdfs:label ?productFeature . 	"
			+ "bsbm-inst:Product1 bsbm:productPropertyTextual1 ?propertyTextual1 . 	"
			+ "bsbm-inst:Product1 bsbm:productPropertyTextual2 ?propertyTextual2 . "
			+ "bsbm-inst:Product1 bsbm:productPropertyTextual3 ?propertyTextual3 . 	"
			+ "bsbm-inst:Product1 bsbm:productPropertyNumeric1 ?propertyNumeric1 . 	"
			+ "bsbm-inst:Product1 bsbm:productPropertyNumeric2 ?propertyNumeric2 . 	"
			+ "OPTIONAL { bsbm-inst:Product1 bsbm:productPropertyTextual4 ?propertyTextual4 }  "
			+ "OPTIONAL { bsbm-inst:Product1 bsbm:productPropertyTextual5 ?propertyTextual5 }  "
			+ "OPTIONAL { bsbm-inst:Product1 bsbm:productPropertyNumeric4 ?propertyNumeric4 } "
			+ "}";
	
	
	
	private String q4 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>"
			+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>"
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
			+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
			+ "SELECT DISTINCT ?product ?label ?propertyTextual"
			+ "WHERE {"
			+ " { "
			+ " ?product rdfs:label ?label ."
			+ " ?product rdf:type bsbm-inst:ProductType1 ."
			+ " ?product bsbm:productFeature bsbm-inst:ProductFeature39 ."
			+ "	?product bsbm:productFeature bsbm-inst:ProductFeature41 ."
			+ " ?product bsbm:productPropertyTextual1 ?propertyTextual ."
			+ "	?product bsbm:productPropertyNumeric1 ?p1 ."
			+ "	FILTER ( ?p1 > 2 )"
			+ " } UNION {"
			+ " ?product rdfs:label ?label ."
			+ " ?product rdf:type bsbm-inst:ProductType1  ."
			+ " ?product bsbm:productFeature bsbm-inst:ProductFeature39 ."
			+ "	?product bsbm:productFeature bsbm-inst:ProductFeature46  ."
			+ " ?product bsbm:productPropertyTextual1 ?propertyTextual ."
			+ "	?product bsbm:productPropertyNumeric2 ?p2 ."
			+ "	FILTER ( ?p2 > 3 ) "
			+ " } "
			+ "} "
			+ "ORDER BY ?label "
			+ "OFFSET 5 " + "LIMIT 10 ";

	@Test
	public void test1() {
		String query = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT * WHERE {<http://uri1> ?p '1'^^xsd:int}";	
	
		Op op = new  AlgebraGenerator().compile(QueryFactory.create(query));
		
		Op newOp = beauty.compileToBeauty(QueryFactory.create(query));		
		
		
		System.out.println("old:" + op.hashCode() + op.toString());
		System.out.println("new: " + newOp.hashCode() + newOp.toString());
		System.out.println("new: " + OpAsQuery.asQuery(newOp));
		

	}
	
	@Test
	public void testq2() {
		String query = this.q2;
		
		Op op = new  AlgebraGenerator().compile(QueryFactory.create(query));
		
		Op newOp = beauty.compileToBeauty(QueryFactory.create(query));		
		
		System.out.println("old:" + op.hashCode() + op.toString());
		System.out.println("new: " + newOp.hashCode() + newOp.toString());
		System.out.println("new: " + OpAsQuery.asQuery(newOp));
		

	}
	@Test
	public void testq4() {
		String query = this.q4;
		
		Op op = new  AlgebraGenerator().compile(QueryFactory.create(query));
		
		Op newOp = beauty.compileToBeauty(QueryFactory.create(query));		
		
		System.out.println("old:" + op.hashCode() + op.toString());
		System.out.println("new: " + newOp.hashCode() + newOp.toString());
		System.out.println("new: " + OpAsQuery.asQuery(newOp));
		

	}

}
