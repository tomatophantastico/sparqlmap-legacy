package org.aksw.sparqlmap.bsbmtestcases;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.XSD;


/**
 * this class contains queries and the expected results
 * @author joerg
 *
 */
public class BSBM100k {
	
	
	public static String q1 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\n" + 
			"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
			"\n" + 
			"SELECT DISTINCT ?product ?label\n" + 
			"WHERE { \n" + 
			"    ?product rdfs:label ?label .\n" + 
			"    ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType11> .\n" + 
			"    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature439> . \n" + 
			"    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature4> . \n" + 
			"    ?product bsbm:productPropertyNumeric1 ?value1 . \n" + 
			"        FILTER (?value1 > 136) \n" + 
			"        }\n" + 
			"ORDER BY ?label\n" + 
			"LIMIT 10\n";
	
	public static Multimap<String,String> q1Res(){
		Multimap<String,String> res = HashMultimap.create();
		res.put("product", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer5/Product185");
		res.put("product", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer3/Product117");
		res.put("label", "grubbily atherosclerosis abstemiously");
		res.put("label", "hitchhiked");
		return res;
	}
	
	public static int q1count = 2;
	
	
	public static String q2 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\n" + 
			"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
			"\n" + 
			"SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3\n" + 
			" ?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4 \n" + 
			"WHERE {\n" + 
			"    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> rdfs:label ?label .\n" + 
			"    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> rdfs:comment ?comment .\n" + 
			"    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> bsbm:producer ?p .\n" + 
			"    ?p rdfs:label ?producer .\n" + 
			"    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> dc:publisher ?p . \n" + 
			"    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> bsbm:productFeature ?f .\n" + 
			"    ?f rdfs:label ?productFeature .\n" + 
			"    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> bsbm:productPropertyTextual1 ?propertyTextual1 .\n" + 
			"    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> bsbm:productPropertyTextual2 ?propertyTextual2 .\n" + 
			"    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> bsbm:productPropertyTextual3 ?propertyTextual3 .\n" + 
			"    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> bsbm:productPropertyNumeric1 ?propertyNumeric1 .\n" + 
			"    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> bsbm:productPropertyNumeric2 ?propertyNumeric2 .\n" + 
			"    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> bsbm:productPropertyTextual4 ?propertyTextual4 }\n" + 
			"    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> bsbm:productPropertyTextual5 ?propertyTextual5 }\n" + 
			"    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product32> bsbm:productPropertyNumeric4 ?propertyNumeric4 }\n" + 
			"}";
	
	public static Multimap<String,String> q2Res(){
		Multimap<String,String> res = HashMultimap.create();
		res.put("label", "interpretable farmed");
		res.put("comment", "apologizers dinguses upbraids narcoleptic soybeans tzarism schmaltzes heralds pleurisy bastardization shewn rarefies unpacified toff greenhorn plussages pronunciamentos ensue explicator boastfulness hired dissipating virtuosities matchmaking complaining skirmisher malignly proprietorial forewoman formulator landsman gladdened carefuller anthologist disorder bibliographer zikurat inflictions slumberers paramour eternities noisily maleficent vialled abusers dinkier bruises befools breathier antiphons fagots gifts financially yearnings dhole moonish pretenders actualized unload overhastily fullest ventricles organizers guilelessness mordent reclamations trichloromethanes telecommunications enshrouding regathering cyclopedias businesswomen gridlock chirpier currycombed fizzers albinisms supplementation inhabitance proconsuls anglophilia salerooms accelerates reweds neuralgias screeners exuded muzzling reorder crestings embarrassing poaching enflamed daintiness hayfields greyhounds unrhymed handfuls flagellated revivers suspecting puzzles availability pouts elkhounds hexapody stoopingly extorsive bugger deveining forager suckers badness inflation dimorphic wergeld ounces renomination zested misdiagnosed");
		res.put("producer", "crossbreed breakneck partying");
		res.put("productFeature", "alveolate");
		res.put("productFeature", "blundering");
		res.put("productFeature", "unnumbered");
		res.put("propertyNumeric1", "533^^" + XSD.integer.getURI() );
		res.put("propertyNumeric2", "1869^^" + XSD.integer.getURI());
		return res;
	}
	
	public static int q2count = 33;
	
	public static String q3 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\n" + 
			"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
			"\n" + 
			"SELECT ?product ?label\n" + 
			"WHERE {\n" + 
			"    ?product rdfs:label ?label .\n" + 
			"    ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType27> .\n" + 
			"	?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature1339> .\n" + 
			"	?product bsbm:productPropertyNumeric1 ?p1 .\n" + 
			"	FILTER ( ?p1 > 276 ) \n" + 
			"	?product bsbm:productPropertyNumeric3 ?p3 .\n" + 
			"	FILTER (?p3 < 263 )\n" + 
			"    OPTIONAL { \n" + 
			"        ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature79> .\n" + 
			"        ?product rdfs:label ?testVar }\n" + 
			"    FILTER (!bound(?testVar)) \n" + 
			"}\n" + 
			"ORDER BY ?label\n" + 
			"LIMIT 10";
	public static int q3count = 1;
	public static Multimap<String,String> q3Res(){
		Multimap<String,String> res = HashMultimap.create();
		res.put("product", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product22");
		res.put("label", "yowls");

		return res;
	}
	
	
	public static String q4 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\n" + 
			"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
			"\n" + 
			"SELECT DISTINCT ?product ?label ?propertyTextual ?pf\n" + 
			"WHERE {\n" + 
			"    { \n" + 
			"       ?product rdfs:label ?label .\n" + 
			"       ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType7> .\n" + 
			"       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature11> .\n" + 
			"	   ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature10> .\n" + 
			"       ?product bsbm:productPropertyTextual1 ?propertyTextual .\n" + 
			"	   ?product bsbm:productPropertyNumeric1 ?p1 .\n" + 
			"	   FILTER ( ?p1 > 211 )\n" + 
			"    } UNION {\n" + 
			"       ?product rdfs:label ?label .\n" + 
			"       ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType7> .\n" + 
			"       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature7> .\n" + 
			"	   ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature14> .\n" + 
			"       ?product bsbm:productPropertyTextual1 ?propertyTextual .\n" + 
			"	   ?product bsbm:productPropertyNumeric2 ?p2 .\n" + 
			"	   FILTER ( ?p2> 376 ) \n" + 
			"    } \n" + 
			"}\n" + 
			"ORDER BY ?label\n" + 
			"OFFSET 1\n" + 
			"LIMIT 10";
	public static int q4count = 2;
	public static Multimap<String,String> q4Res(){
		Multimap<String,String> res = HashMultimap.create();
		res.put("product", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer3/Product125");
		res.put("product", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer4/Product143");
		res.put("label", "toleration medullar");
		res.put("label", "upgrading unearths organizationally");
		res.put("propertyTextual", "jimsonweed owners casings rooting subsided doorsteps sutras clicks janitors parallelling capitulating youngling durably fosterer");
		res.put("propertyTextual", "criticizers farness decoyed preinsert deadeye ruminates electrocute cleverish pleading enclose hardset nobby judgeships");
		return res;
	}
	
	
	public static String q5 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
			"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"\n" + 
			"SELECT DISTINCT ?product ?productLabel\n" + 
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
			"LIMIT 5";
	public static int q5count = 1;
	public static Multimap<String,String> q5Res(){
		Multimap<String,String> res = HashMultimap.create();
		res.put("product", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer3/Product112");
		res.put("productLabel", "acknowledging");
		return res;
	}
	
	
	public static String q7 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX rev: <http://purl.org/stuff/rev#>\n" + 
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
			"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
			"\n" + 
			"SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle \n" + 
			"       ?reviewer ?revName ?rating1 ?rating2\n" + 
			"WHERE { \n" + 
			"	<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer5/Product178> rdfs:label ?productLabel .\n" + 
			"    OPTIONAL {\n" + 
			"        ?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer5/Product178> .\n" + 
			"		?offer bsbm:price ?price .\n" + 
			"		?offer bsbm:vendor ?vendor .\n" + 
			"		?vendor rdfs:label ?vendorTitle .\n" + 
			"        ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE> .\n" + 
			"        ?offer dc:publisher ?vendor . \n" + 
			"        ?offer bsbm:validTo ?date .\n" + 
			"        FILTER (?date > \"2008-06-20T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> )\n" + 
			"    }\n" + 
			"    OPTIONAL {\n" + 
			"	?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer5/Product178> .\n" + 
			"	?review rev:reviewer ?reviewer .\n" + 
			"	?reviewer foaf:name ?revName .\n" + 
			"	?review dc:title ?revTitle .\n" + 
			"    OPTIONAL { ?review bsbm:rating1 ?rating1 . }\n" + 
			"    OPTIONAL { ?review bsbm:rating2 ?rating2 . } \n" + 
			"    }\n" + 
			"}\n" + 
			" ORDER BY ?review";
	public static int q7count = 7;
	public static Multimap<String,String> q7Res(){
		Multimap<String,String> res = HashMultimap.create();
		res.put("productLabel", "ingeniousness");
		res.put("review", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review151");
		res.put("revTitle", "immix prayerfully airbusses commercialist nationalization scarpering gametic viciously teenybopper rewoven phlegmatic mafioso stereoscope veering");
		res.put("revName", "Linda-Nada");
		res.put("revName", "Bairei-Michihiro");
		res.put("revName", "Carne-Sylvian");
		res.put("revName", "Gekko-Liuz");
		res.put("review", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review2739");
		return res;
	}
	
	
	
	public static String q8 = "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
			"PREFIX rev: <http://purl.org/stuff/rev#>\n" + 
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
			"\n" + 
			"SELECT ?title ?text ?reviewDate ?reviewer ?reviewerName ?rating1 ?rating2 ?rating3 ?rating4 \n" + 
			"WHERE { \n" + 
			"	?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product213> .\n" + 
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
	public static int q8count = 0;
	public static Multimap<String,String> q8Res(){
		Multimap<String,String> res = HashMultimap.create();
		
		return res;
	}
	
	public static String q9 = "PREFIX rev: <http://purl.org/stuff/rev#>\n" + 
			"\n" + 
			"DESCRIBE ?x\n" + 
			"WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review1623> rev:reviewer ?x }\n" + 
			"";
	
	public static Set<String> q9Res(){
		Set<String> someStrings = new HashSet<String>();
		someStrings.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review1631");
		someStrings.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Reviewer83");
		someStrings.add("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/RatingSite1");
		someStrings.add("cd82079fd9a4d3232c0bbcb71be6cdad79becb");
		someStrings.add("Lal-Lindtraud");
		return someStrings;
	}
	
	public static String q10 = "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n" + 
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
			"\n" + 
			"SELECT DISTINCT ?offer ?price\n" + 
			"WHERE {\n" + 
			"	?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer4/Product149> .\n" + 
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
	public static int q10count = 0;
	public static Multimap<String,String> q10Res(){
		Multimap<String,String> res = HashMultimap.create();
//		res.put("", "");
//		res.put("", "");
//		res.put("", "");
		return res;
	}
	
	public static String q11 = "SELECT ?property ?hasValue ?isValueOf\n" + 
			"WHERE {\n" + 
			"  { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor3/Offer5096> ?property ?hasValue }\n" + 
			"  UNION\n" + 
			"  { ?isValueOf ?property <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor3/Offer5096> }\n" + 
			"}";
	public static int q11count = 11;
	public static Multimap<String,String> q11Res(){
		Multimap<String,String> res = HashMultimap.create();
		res.put("property", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/vendor");
		res.put("property", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product");
		res.put("property", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/offerWebpage");
		res.put("hasValue", "2008-05-17^^http://www.w3.org/2001/XMLSchema#date");
		res.put("hasValue", "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Offer");
		return res;
	}
	
	public static String q12 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
			"PREFIX rev: <http://purl.org/stuff/rev#>\n" + 
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
			"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" + 
			"PREFIX bsbm-export: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/>\n" + 
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + 
			"\n" + 
			"CONSTRUCT {  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm-export:product ?productURI .\n" + 
			"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm-export:productlabel ?productlabel .\n" + 
			"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm-export:vendor ?vendorname .\n" + 
			"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm-export:vendorhomepage ?vendorhomepage . \n" + 
			"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm-export:offerURL ?offerURL .\n" + 
			"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm-export:price ?price .\n" + 
			"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm-export:deliveryDays ?deliveryDays .\n" + 
			"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm-export:validuntil ?validTo } \n" + 
			"WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm:product ?productURI .\n" + 
			"        ?productURI rdfs:label ?productlabel .\n" + 
			"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm:vendor ?vendorURI .\n" + 
			"        ?vendorURI rdfs:label ?vendorname .\n" + 
			"        ?vendorURI foaf:homepage ?vendorhomepage .\n" + 
			"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm:offerWebpage ?offerURL .\n" + 
			"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm:price ?price .\n" + 
			"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm:deliveryDays ?deliveryDays .\n" + 
			"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor2/Offer4275> bsbm:validTo ?validTo }";

	public static Set<String> q12Res(){
		Set<String> res = new HashSet<String>();
		res.add("");
		return res;
	}
	
	
	
}
