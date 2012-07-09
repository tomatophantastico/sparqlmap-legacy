package org.aksw.sparqlmap;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

import org.aksw.sparqlmap.RDB2RDF.ReturnType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

public abstract class BSBMTest extends BaseTest {

	private Logger log = LoggerFactory.getLogger(BSBMTest.class);

	public static final String q1_old = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
			+

			" SELECT DISTINCT ?product ?lbl"
			+ " WHERE {"
			+ " ?product rdfs:label ?lbl ."
			+ " ?product a bsbm-inst:ProductType1 ."
			+ " ?product bsbm:productFeature bsbm-inst:ProductFeature39 ."
			+ " ?product bsbm:productFeature bsbm-inst:ProductFeature41 ."
			+ " ?product bsbm:productPropertyNumeric1 ?value1 ."
			+ "	FILTER (?value1 > 1)" + "	}" + "ORDER BY ?lbl " + "LIMIT 10 ";

	
	public static String q1 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  SELECT DISTINCT ?product ?label WHERE {      ?product rdfs:label ?label .     ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1446> .     ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature34439> .      ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature892> .      ?product bsbm:productPropertyNumeric1 ?value1 .  	FILTER (?value1 > 136)  	} ORDER BY ?label LIMIT 10 ";
	private String sql1 = "SELECT distinct nr, label FROM product p, producttypeproduct ptp WHERE p.nr = ptp.product AND ptp.productType=1 AND propertyNum1 > 1 AND p.nr IN (SELECT distinct product FROM productfeatureproduct WHERE productFeature=39) AND p.nr IN (SELECT distinct product FROM productfeatureproduct WHERE productFeature=41) ORDER BY label LIMIT 10;";
	
	private String q2_old = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
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
	public static String q2 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX dc: <http://purl.org/dc/elements/1.1/>  SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3  ?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4  WHERE {     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> rdfs:label ?label .     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> rdfs:comment ?comment .     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> bsbm:producer ?p .     ?p rdfs:label ?producer .     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> dc:publisher ?p .      <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> bsbm:productFeature ?f .     ?f rdfs:label ?productFeature .     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> bsbm:productPropertyTextual1 ?propertyTextual1 .     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> bsbm:productPropertyTextual2 ?propertyTextual2 .     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> bsbm:productPropertyTextual3 ?propertyTextual3 .     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> bsbm:productPropertyNumeric1 ?propertyNumeric1 .     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> bsbm:productPropertyNumeric2 ?propertyNumeric2 .     OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> bsbm:productPropertyTextual4 ?propertyTextual4 }     OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> bsbm:productPropertyTextual5 ?propertyTextual5 }     OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1230/Product61272> bsbm:productPropertyNumeric4 ?propertyNumeric4 } } ";
	private String sql2 = "SELECT pt.label, pt.comment, pt.producer, productFeature, propertyTex1, propertyTex2, propertyTex3,propertyNum1 AS 'propertyNumeric1', propertyNum2, propertyTex4, propertyTex5, propertyNum4 FROM product pt, producer pr, productfeatureproduct pfp WHERE pt.nr=1 AND pt.nr=pfp.product AND pt.producer=pr.nr;";

	private String q3_old = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  "
			+ "SELECT ?product ?label WHERE {  "
			+ "?product rdfs:label ?label .  "
			+ "?product a bsbm-inst:ProductType1 . 	"
			+ "?product bsbm:productFeature bsbm-inst:ProductFeature39 . 	"
			+ "?product bsbm:productPropertyNumeric1 ?p1 . 	"
			+ "FILTER ( ?p1 > 1)  	"
			+ "?product bsbm:productPropertyNumeric3 ?p3 . 	"
			+ "FILTER (?p3 < 2 )  "
			+ "OPTIONAL {   ?product bsbm:productFeature bsbm-inst:ProductFeature41 .  ?product rdfs:label ?testVar }  "
			+ "FILTER (!bound(?testVar))  } " + "ORDER BY ?label LIMIT 10";
	
	public static String q3 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  SELECT ?product ?label WHERE {     ?product rdfs:label ?label .     ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1267> . 	?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature8000> . 	?product bsbm:productPropertyNumeric1 ?p1 . 	FILTER ( ?p1 > 228 )  	?product bsbm:productPropertyNumeric3 ?p3 . 	FILTER (?p3 < 156 )     OPTIONAL {          ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature8001> .         ?product rdfs:label ?testVar }     FILTER (!bound(?testVar))  } ORDER BY ?label LIMIT 10  ";

	
	
	
	
	public static String q4 ="PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> SELECT DISTINCT ?product ?label ?propertyTextual WHERE {     {        ?product rdfs:label ?label .        ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType87> .        ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature2675> .            ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature557> .        ?product bsbm:productPropertyTextual1 ?propertyTextual .            ?product bsbm:productPropertyNumeric1 ?p1 .            FILTER ( ?p1 > 9 )     } UNION {        ?product rdfs:label ?label .        ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType87> .        ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature2675> .            ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature545> .        ?product bsbm:productPropertyTextual1 ?propertyTextual .            ?product bsbm:productPropertyNumeric2 ?p2 .            FILTER ( ?p2> 1 )     } } ORDER BY ?label OFFSET 5 LIMIT 10  ";
	
	private String sql4 = "SELECT distinct p.nr, p.label, p.propertyTex1 FROM product p, producttypeproduct ptp WHERE p.nr=ptp.product AND ptp.productType=1 AND p.nr IN (SELECT distinct product FROM productfeatureproduct WHERE productFeature=39) AND ((propertyNum1>2 AND p.nr IN (SELECT distinct product FROM productfeatureproduct WHERE productFeature=41)) OR (propertyNum2>3 AND p.nr IN (SELECT distinct product FROM productfeatureproduct WHERE productFeature=46))) ORDER BY label LIMIT 10 OFFSET 5;";
	
	private String q5_old = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
			+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>  "
			+ "SELECT DISTINCT ?product ?productLabel "
			+ "WHERE {  	"
			+ "?product rdfs:label ?productLabel .  "
			+ "FILTER (bsbm-inst:Product1 != ?product) 	"
			+ "bsbm-inst:Product1 bsbm:productFeature ?prodFeature . 	"
			+ "?product bsbm:productFeature ?prodFeature . 	"
			+ "bsbm-inst:Product1 bsbm:productPropertyNumeric1 ?origProperty1 . 	"
			+ "?product bsbm:productPropertyNumeric1 ?simProperty1 . 	"
			+ "FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120)) 	"
			+ "bsbm-inst:Product1 bsbm:productPropertyNumeric2 ?origProperty2 . 	"
			+ "?product bsbm:productPropertyNumeric2 ?simProperty2 . 	"
			+ "FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170)) } "
			+ "" + "ORDER BY ?productLabel LIMIT 5";
	
	public static String q5 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
			"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"\n" + 
			"SELECT DISTINCT ?product ?productLabel\n" + 
			"WHERE { \n" + 
			"	?product rdfs:label ?productLabel .\n" + 
			"    FILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer25/Product1190> != ?product)\n" + 
			"	<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer25/Product1190> bsbm:productFeature ?prodFeature .\n" + 
			"	?product bsbm:productFeature ?prodFeature .\n" + 
			"	<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer25/Product1190> bsbm:productPropertyNumeric1 ?origProperty1 .\n" + 
			"	?product bsbm:productPropertyNumeric1 ?simProperty1 .\n" + 
			"	FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120))\n" + 
			"	<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer25/Product1190> bsbm:productPropertyNumeric2 ?origProperty2 .\n" + 
			"	?product bsbm:productPropertyNumeric2 ?simProperty2 .\n" + 
			"	FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170))\n" + 
			"}\n" + 
			"ORDER BY ?productLabel\n" + 
			"LIMIT 5";
	
	private String sql5 = "SELECT distinct p.nr, p.label FROM product p, product po, (Select distinct pfp1.product FROM productfeatureproduct pfp1, (SELECT productFeature FROM productfeatureproduct WHERE product=1) pfp2 WHERE pfp2.productFeature=pfp1.productFeature) pfp WHERE p.nr=pfp.product AND po.nr=1 AND p.nr!=po.nr AND p.propertyNum1<(po.propertyNum1+120) AND p.propertyNum1>(po.propertyNum1-120) AND p.propertyNum2<(po.propertyNum2+170) AND p.propertyNum2>(po.propertyNum2-170) ORDER BY label LIMIT 5;";		
			
	public static final String q6 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
			+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>  "
			+ "SELECT ?product ?label WHERE { 	"
			+ "?product rdfs:label ?label .  "
			+ "?product rdf:type bsbm:Product . 	"
			+ "FILTER regex(?label, 'and') }";
	
	private String sql6 = "SELECT nr, label FROM product WHERE label like \"%and%\";";
	
	private String q7_old = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+ "PREFIX rev: <http://purl.org/stuff/rev#> "
			+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
			+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
			+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>  "
			+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>  "
			+ "SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle   ?reviewer ?revName ?rating1 ?rating2 "
			+ "WHERE {  	"
			+ "bsbm-inst:Product1 rdfs:label ?productLabel .  "
			+ "OPTIONAL {  ?offer bsbm:product bsbm-inst:Product1 . 	"
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
			+ "?review dc:title ?revTitle .  OPTIONAL { ?review bsbm:rating1 ?rating1 . }  OPTIONAL { ?review bsbm:rating2 ?rating2 . }   } } limit 10000" ;
	
	public static String q7 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX rev: <http://purl.org/stuff/rev#>\n" + 
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
			"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
			"\n" + 
			"SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle \n" + 
			"       ?reviewer ?revName ?rating1 ?rating2\n" + 
			"WHERE { \n" + 
			"	<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer12/Product554> rdfs:label ?productLabel .\n" + 
			"    OPTIONAL {\n" + 
			"        ?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer12/Product554> .\n" + 
			"		?offer bsbm:price ?price .\n" + 
			"		?offer bsbm:vendor ?vendor .\n" + 
			"		?vendor rdfs:label ?vendorTitle .\n" + 
			"        ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE> .\n" + 
			"        ?offer dc:publisher ?vendor . \n" + 
			"        ?offer bsbm:validTo ?date .\n" + 
			"        FILTER (?date > \"2008-06-20T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> )\n" + 
			"    }\n" + 
			"    OPTIONAL {\n" + 
			"	?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer12/Product554> .\n" + 
			"	?review rev:reviewer ?reviewer .\n" + 
			"	?reviewer foaf:name ?revName .\n" + 
			"	?review dc:title ?revTitle .\n" + 
			"    OPTIONAL { ?review bsbm:rating1 ?rating1 . }\n" + 
			"    OPTIONAL { ?review bsbm:rating2 ?rating2 . } \n" + 
			"    }\n" + 
			"}";
	

	public static String q8 ="PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
			"PREFIX rev: <http://purl.org/stuff/rev#>\n" + 
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
			"\n" + 
			"SELECT ?title ?text ?reviewDate ?reviewer ?reviewerName ?rating1 ?rating2 ?rating3 ?rating4 \n" + 
			"WHERE { \n" + 
			"	?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer16/Product738> .\n" + 
			"	?review dc:title ?title .\n" + 
			"	?review rev:text ?text .\n" + 
			"	FILTER langMatches( lang(?text), \"EN\" ) \n" + 
			"	?review bsbm:reviewDate ?reviewDate .\n" + 
			"	?review rev:reviewer ?reviewer .\n" + 
			"	?reviewer foaf:name ?reviewerName .\n" + 
			"	OPTIONAL { ?review bsbm:rating1 ?rating1 . }\n" + 
			"	OPTIONAL { ?review bsbm:rating2 ?rating2 . }\n" + 
			"	OPTIONAL { ?review bsbm:rating3 ?rating3 . }\n" + 
			"	OPTIONAL { ?review bsbm:rating4 ?rating4 . }\n" + 
			"}\n" + 
			"ORDER BY DESC(?reviewDate)\n" + 
			"LIMIT 20";
	
	
	
	private String q9_old = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+ "PREFIX rev: <http://purl.org/stuff/rev#>  "
			+ "DESCRIBE ?x WHERE { bsbm-inst:Review1 rev:reviewer ?x}";
	public static String q9 ="PREFIX rev: <http://purl.org/stuff/rev#>  DESCRIBE ?x WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite21/Review217211> rev:reviewer ?x } ";
	private String q9_as_select = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+ "PREFIX rev: <http://purl.org/stuff/rev#>  "
			+ "Select ?x ?y ?z WHERE { bsbm-inst:Review1 rev:reviewer ?x . ?x ?y ?z}";
	private String q10_old = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
			+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>  "
			+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>  "
			+ "SELECT DISTINCT ?offer ?price WHERE { 	"
			+ "?offer bsbm:product bsbm-inst:Product1 . 	"
			+ "?offer bsbm:vendor ?vendor .  "
			+ "?offer dc:publisher ?vendor . 	"
			+ "?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> . 	"
			+ "?offer bsbm:deliveryDays ?deliveryDays . 	"
			+ "FILTER (?deliveryDays <= 3) 	"
			+ "?offer bsbm:price ?price .  "
			+ "?offer bsbm:validTo ?date .  "
			+ "FILTER (?date > \"2005-05-01T00:00:00Z\"^^xsd:dateTime  ) } "
			+ "ORDER BY xsd:double(str(?price)) LIMIT 10";
	public static String q10 = "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n" + 
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
			"\n" + 
			"SELECT DISTINCT ?offer ?price\n" + 
			"WHERE {\n" + 
			"	?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer34/Product1546> .\n" + 
			"	?offer bsbm:vendor ?vendor .\n" + 
			"    ?offer dc:publisher ?vendor .\n" + 
			"	?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .\n" + 
			"	?offer bsbm:deliveryDays ?deliveryDays .\n" + 
			"	FILTER (?deliveryDays <= 3)\n" + 
			"	?offer bsbm:price ?price .\n" + 
			"    ?offer bsbm:validTo ?date .\n" + 
			"    FILTER (?date > \"2008-06-20T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> )\n" + 
			"}\n" + 
			"ORDER BY xsd:double(str(?price))\n" + 
			"LIMIT 10";
	
	
	
	public static String q11  = "SELECT ?property ?hasValue ?isValueOf " +
			"WHERE {   { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor244/Offer480636> ?property ?hasValue }   UNION   { ?isValueOf ?property <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor244/Offer480636> } } ";
	
	
	
	private String q12_ = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+ "PREFIX rev: <http://purl.org/stuff/rev#> "
			+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
			+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
			+ "PREFIX bsbm-export: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/> "
			+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>  "
			+ "SELECT ?productURI  ?productlabel  ?vendorname ?vendorhomepage ?offerURL ?price  ?deliveryDays  ?validTo   " +
			"WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:product ?productURI .       " +
			" ?productURI rdfs:label ?productlabel .        " +
			" <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:vendor ?vendorURI .      " +
			"  ?vendorURI rdfs:label ?vendorname .         ?vendorURI foaf:homepage ?vendorhomepage .    " +
			"     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:offerWebpage ?offerURL .         " +
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:price ?price .         " +
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:deliveryDays ?deliveryDays .         " +
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:validTo ?validTo } ";
	
	private String q12_real = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rev: <http://purl.org/stuff/rev#> PREFIX foaf: <http://xmlns.com/foaf/0.1/> PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> PREFIX bsbm-export: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/> PREFIX dc: <http://purl.org/dc/elements/1.1/>  " +
			"CONSTRUCT {  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:product ?productURI . " +
			"     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:productlabel ?productlabel .   " +
			"           <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:vendor ?vendorname .     " +
			"         <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:vendorhomepage ?vendorhomepage .  " +
			"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:offerURL ?offerURL . " +
			"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:price ?price .        " +
			"      <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:deliveryDays ?deliveryDays .   " +
			"           <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:validuntil ?validTo }  " +
			"WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:product ?productURI .       " +
			" ?productURI rdfs:label ?productlabel .        " +
			" <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:vendor ?vendorURI .      " +
			"  ?vendorURI rdfs:label ?vendorname .         ?vendorURI foaf:homepage ?vendorhomepage .    " +
			"     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:offerWebpage ?offerURL .         " +
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:price ?price .         " +
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:deliveryDays ?deliveryDays .         " +
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:validTo ?validTo } ";
	private String q12_old = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+ "PREFIX rev: <http://purl.org/stuff/rev#> "
			+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
			+ "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
			+ "PREFIX bsbm-export: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/> "
			+ "PREFIX dc: <http://purl.org/dc/elements/1.1/>  "
			+ "CONSTRUCT {  bsbm-inst:Offer1 bsbm-export:product ?productURI .   bsbm-inst:Offer1 bsbm-export:productlabel ?productlabel .   bsbm-inst:Offer1 bsbm-export:vendor ?vendorname .   bsbm-inst:Offer1 bsbm-export:vendorhomepage ?vendorhomepage .    bsbm-inst:Offer1 bsbm-export:offerURL ?offerURL .   bsbm-inst:Offer1 bsbm-export:price ?price .   bsbm-inst:Offer1 bsbm-export:deliveryDays ?deliveryDays .   bsbm-inst:Offer1 bsbm-export:validuntil ?validTo }  WHERE {  bsbm-inst:Offer1 bsbm:product ?productURI .  ?productURI rdfs:label ?productlabel .   bsbm-inst:Offer1 bsbm:vendor ?vendorURI .  ?vendorURI rdfs:label ?vendorname .  ?vendorURI foaf:homepage ?vendorhomepage .   bsbm-inst:Offer1 bsbm:offerWebpage ?offerURL .   bsbm-inst:Offer1 bsbm:price ?price .   bsbm-inst:Offer1 bsbm:deliveryDays ?deliveryDays .   bsbm-inst:Offer1 bsbm:validTo ?validTo } ";
	public static String q12 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rev: <http://purl.org/stuff/rev#> PREFIX foaf: <http://xmlns.com/foaf/0.1/> PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> PREFIX bsbm-export: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/> PREFIX dc: <http://purl.org/dc/elements/1.1/>  CONSTRUCT {  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:product ?productURI .              <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:productlabel ?productlabel .              <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:vendor ?vendorname .              <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:vendorhomepage ?vendorhomepage .               <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:offerURL ?offerURL .              <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:price ?price .              <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:deliveryDays ?deliveryDays .              <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm-export:validuntil ?validTo }  WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:product ?productURI .         ?productURI rdfs:label ?productlabel .         <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:vendor ?vendorURI .         ?vendorURI rdfs:label ?vendorname .         ?vendorURI foaf:homepage ?vendorhomepage .         <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:offerWebpage ?offerURL .         <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:price ?price .         <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:deliveryDays ?deliveryDays .         <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor679/Offer1339557> bsbm:validTo ?validTo }";

	@Test
	public void query1Test() {
		
		ArrayList checkKeys = new ArrayList();
		checkKeys.add("label");

		processQuery("BSBM query 1", q1, sql1, checkKeys);

	}

	@Test
	public void query2Test() {
		
		ArrayList checkKeys = new ArrayList();
		checkKeys.add("label");
		checkKeys.add("propertyNumeric1");
		
		processQuery("BSBM query 2", q2, sql2, checkKeys);
	}

	@Test
	public void query3Test() {

		processQuery("BSBM query 3", q3);
	}

	@Test
	public void query4Test() {
		ArrayList checkKeys = new ArrayList();
		checkKeys.add("label");
		
		processQuery("BSBM query 4", q4, sql4, checkKeys);
	}

	@Test
	public void query5Test() {
		ArrayList checkKeys = new ArrayList();
		checkKeys.add("productLabel");
		
		processQuery("BSBM query 5", q5, sql5, checkKeys);
	}

	@Test
	public void query6Test() {

		ArrayList checkKeys = new ArrayList();
		checkKeys.add("product");
		
		//processQuery("BSBM query 6", q6, sql6, checkKeys);
		processQuery("BSBM query 6", q6);
	}

	@Test
	public void query7Test() {

		processQuery("BSBM query 7", q7);
	}

	@Test
	public void query8Test() {
		processQuery("BSBM query 8", q8);
	}

	@Test
	public void query9Test() {
		processQuery("BSBM query 9", q9);
	}

	@Test
	public void query10Test() {
		processQuery("BSBM query 10", q10);
	}

	@Test
	public void query11Test() {
		processQuery("BSBM query 11", q11);

	}

	@Test
	public void query12Test() {
		processQuery("BSBM query 12", q12);
	}
	
	
	private RDB2RDF r2r = new RDB2RDF("bsbm.r2rml");

	public String processQuery(String queryShortname, String query) {

		log.info(queryShortname);
		log.info(query);
		// check if it is a valid SPARQL query
		Query pQuery = QueryFactory.create(query);
		String res = "";
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			r2r.executeSparql(query, ReturnType.XML, out);
			res = out.toString();
			
			log.info(res);
		} catch (Exception e) {
			log.info("Error converting the query:", e);
		}
		log.info("*******************************");
		return res;
	}
	
	public abstract void processQuery(String queryShortname, String query, String sqlQuery, ArrayList checkKeys);
	
//	
//	{
//		
//		log.info("*************************************************************************");
//		log.info("Query:      " + queryShortname);
//		log.info("SPARQL:     " + query);
//		log.info("BSBM SQL:   " + sqlQuery);
//		
//		// check if it is a valid SPARQL query
//		Query pQuery = QueryFactory.create(query);
//		String rewrittenSql = "";
//		try {
//			rewrittenSql = mapper.rewrite(query);
//			log.info("Result SQL: " + rewrittenSql);
//		} catch (Exception e) {
//			log.error("Error converting the query:",e);
//			org.junit.Assert.fail("Error converting the query: "+e.getMessage());
//			
//		}
//		
//		ResultSet rsExpected = null;
//		try {
//			rsExpected = db.executeSQL(sqlQuery).getRs();
//		} catch (Exception e) {
//			org.junit.Assert.fail("Error executing BSBM SQL query: "+e.getMessage());
//		}
//		
//		ResultSet rsRewritten = null;
//		try {
//			rsRewritten = db.executeSQL(rewrittenSql).getRs();
//		} catch (Exception e) {
//			org.junit.Assert.fail("Error executing rewritten SQL query: "+e.getMessage());
//		}
//		
//		int countExpected  = 0;
//		int countRewritten = 0;
//		try {
//			while (rsExpected.next() && rsRewritten.next()) {
//				for (int i=0; i<checkKeys.size(); ++i) {
//					String key = (String)checkKeys.get(i);
//					//log.info("1:"+rsExpected.getString(key));
//					//log.info("2:"+rsRewritten.getString(key));
//					org.junit.Assert.assertEquals(rsExpected.getString(key), rsRewritten.getString(key));
//				}
//			}
//			
//			rsExpected.first();
//			rsRewritten.first();
//			
//			while (rsExpected.next()) {
//				countExpected++;
//			}
//			
//			while (rsRewritten.next()) {
//				countRewritten++;
//			}
//		} catch (Exception e) {
//			org.junit.Assert.fail("Error: "+e.getMessage());
//		}
//		
//		org.junit.Assert.assertEquals(countExpected, countRewritten);
//		
//		log.info("*************************************************************************");
//	}

}
