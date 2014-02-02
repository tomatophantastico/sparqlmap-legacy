package org.aksw.sparqlmap.mapper.subquerymapper.algebra;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;

import org.apache.jena.riot.WebContent;
import org.junit.Test;


public abstract class AlgebraTranslateTest {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AlgebraTranslateTest.class);
	

	
	
	@Test
	public void testLabelQuery(){
		processQuery("rdfs:label", "select * {?s <http://www.w3.org/2000/01/rdf-schema#label> ?o} limit 10");
	}
	
	@Test
	public void testJoinQuery(){
		processQuery("rdfs:label", "select ?text ?offer ?product {?offer <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product> ?product. ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual2>  ?text} limit 10 ");
	}
	@Test
	public void testByTypeQuery(){
		processQuery("rdfs:label", "select ?s {?s a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Offer>} limit 10 ");
	}
	
	
	@Test
	public void testUnionQuery(){
		processQuery("rdfs:label", "select * {{?s <http://www.w3.org/2000/01/rdf-schema#label> ?o} UNION {?s  <http://www.w3.org/2000/01/rdf-schema#comment> ?o }} ");
	}
	@Test
	public void testSimpleUnionQuery(){
		processQuery("rdfs:label", "select * {{?s <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual2> ?o} UNION {?s  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual1> ?o }} ");
	}
	@Test
	public void testspoPQuery(){
		processQuery("spo", "select * {?s ?p ?o} limit 1000");
	}
	@Test
	public void testpoPQuery(){
		processQuery("spo", "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
				" select * {bsbm-inst:Product1 ?p ?o} ");
	}
	
	@Test
	public void testQuestionMarkPQuery(){
		processQuery("rdfs:label", "select * {?s ?p ?o. ?s <http://www.w3.org/2000/01/rdf-schema#label> \"label\" } ");
	}
	
	
	@Test
	public void testQ2NoOpt(){
		processQuery("q7noOpt", "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
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
				+ "bsbm-inst:Product1 dc:publisher ?p .  	"
				+ "bsbm-inst:Product1 bsbm:productFeature ?f . 	"
				+ "bsbm-inst:Product1 bsbm:productPropertyTextual1 ?propertyTextual1 . 	"
				+ "bsbm-inst:Product1 bsbm:productPropertyTextual2 ?propertyTextual2 . "
				+ "bsbm-inst:Product1 bsbm:productPropertyTextual3 ?propertyTextual3 . 	"
				+ "bsbm-inst:Product1 bsbm:productPropertyNumeric1 ?propertyNumeric1 . 	"
				+ "bsbm-inst:Product1 bsbm:productPropertyNumeric2 ?propertyNumeric2 . 	" 
				+ "?f rdfs:label ?productFeature . 	" 
				+ "?p rdfs:label ?producer . "
				+ "}");
	}
	
	
	@Test
	public void testQ2Minimal(){
		processQuery("q7noOpt", "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
				+ "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>  "
				+ "SELECT "
				+ "?label ?p ?producer ?f ?productFeature"
				+ "WHERE {  "
				+ "bsbm-inst:Product1 rdfs:label ?label . 	"
				+ "bsbm-inst:Product1 bsbm:producer ?p . 	"
				+ "bsbm-inst:Product1 dc:publisher ?p .  	"
				+ "bsbm-inst:Product1 bsbm:productFeature ?f . 	"
				+ "bsbm-inst:Product1 bsbm:productPropertyTextual1 ?propertyTextual1 . 	"
				+ "?f rdfs:label ?productFeature . 	" 
				+ "?p rdfs:label ?producer . "
				+ "}");
	}
	
	@Test
	public void testOpt(){
		processQuery("Opt", "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
				+ "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>  "
				+ "SELECT "
				+ "?label ?p ?producer ?f ?productFeature"
				+ "WHERE {  "
				+ "bsbm-inst:Product1 rdfs:label ?label . 	"
				+ "bsbm-inst:Product1 bsbm:producer ?p . 	"
				+ "bsbm-inst:Product1 dc:publisher ?p .  	"
				+ "bsbm-inst:Product1 bsbm:productFeature ?f . 	"
				+ "bsbm-inst:Product1 bsbm:productPropertyTextual1 ?propertyTextual1 . 	"
				+ "OPTIONAL {?f rdfs:label ?productFeature . 	}" 
				+ "}");
	}
	
	
	@Test
	public void testq11_1(){
		String q11 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>"
				+ "SELECT ?property ?hasValue "
				+ "WHERE {"
				+ " <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> ?property ?hasValue "
				+ "}";
		processQuery("q11 part1", q11);
	}
	
	
	@Test
	public void testq11_2(){
		String q11 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>"
				+ "SELECT ?property ?isValueOf "
				+ "WHERE {"
				+ "{ ?isValueOf ?property <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1> }"
				+ "}";
		processQuery("q11 part1", q11);
	}
	
	@Test
	public void testOptSimple(){
		processQuery("Opt", "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
				+ "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>  "
				+ "SELECT "
				+ "?label "
				+ "WHERE {  "
				+ "?prod bsbm:productFeature ?f . 	"
				+ "OPTIONAL {?prod rdfs:label ?label . }	"
				+ "}");
	}
	
	@Test
	public void testVendor(){
		processQuery("vendor","PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "PREFIX rev: <http://purl.org/stuff/rev#> "
				+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>  "
				+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>  "
				+ "SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle   ?reviewer ?revName ?rating1 ?rating2 "
				+ "WHERE {  "
				+ "?offer bsbm:product bsbm-inst:Product1 . 	"
				+ "?offer bsbm:price ?price . 	"
				+ "?offer bsbm:vendor ?vendor . 	"
				+ "?vendor rdfs:label ?vendorTitle .  "
				+ "?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE> .  "
				+ "?offer dc:publisher ?vendor .   "
				+ "?offer bsbm:validTo ?date .  "
				+ "FILTER (?date > \"2005-05-01T00:00:00Z\"^^xsd:dateTime ) }");
	}
	@Test
	public void testVendorOpt(){
		processQuery("vendor","PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "PREFIX rev: <http://purl.org/stuff/rev#> "
				+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
				+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>  "
				+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>  "
				+ "SELECT * "
				+ "WHERE {  	"
				+ "bsbm-inst:Product1 rdfs:label ?productLabel .  "
				+ "OPTIONAL {  " +
				"  ?offer bsbm:product bsbm-inst:Product1 . 	"
				+ "?offer bsbm:price ?price . 	"
				+ "?offer bsbm:vendor ?vendor . 	"
				+ "?vendor rdfs:label ?vendorTitle .  "
				+ "?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE> .  "
				+ "?offer dc:publisher ?vendor .   "
				+ "?offer bsbm:validTo ?date .  "
				+ "FILTER (?date > \"2005-05-01T00:00:00Z\"^^xsd:dateTime )  }  "
				+ "OPTIONAL { 	?review bsbm:reviewFor bsbm-inst:Product1 . 	"
				+ "?review rev:reviewer ?reviewer . 	"
				+ "?reviewer foaf:name ?revName . 	"
				+ "?review dc:title ?revTitle .  OPTIONAL { ?review bsbm:rating1 ?rating1 . }  OPTIONAL { ?review bsbm:rating2 ?rating2 . }   } } limit 100");
	}
	
	
	@Test
	public void testSimpleDescribe(){
		processQuery("simpledescribe", "DESCRIBE <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1>");
	}
	
	@Test
	public void testsomeQuestionMarkPQuery(){
		processQuery("some?p", "CONSTRUCT   { ?s ?p <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/Reviewer11097> .} 	WHERE 	  { ?s ?p <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/Reviewer11097> }");
	}
	
	 	
	
	@Test
	public void testMultiViewResourceCreation(){
		
		processQuery("multiviewresource", "SELECT ?offer WHERE{?offer <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product> <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer251/Product12245>} limit 100");
		
	}
	
	
	@Test
	public void testq5min(){
		String q5min = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>  "
				+ "SELECT DISTINCT ?product ?productLabel "
				+ "WHERE {  	"
				+ "?product rdfs:label ?productLabel .  "
				+ "FILTER (bsbm-inst:Product1 != ?product) 	"
				+ "bsbm-inst:Product1 bsbm:productFeature ?prodFeature . 	"
				+ "?product bsbm:productFeature ?prodFeature . 	" 
				+ "}"
				+ "ORDER BY ?productLabel LIMIT 5";
		processQuery("q5min", q5min);
	}
	
	

	public String processQuery(String shortname, String query) {
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
//		try {
//			r2r.executeSparql(query,WebContent.contentTypeRDFXML, bos,null);
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			log.error("Error:",e);
//		}
//		log.info(bos.toString());
		
		return bos.toString();
		
		
		
		
		
	}

}
