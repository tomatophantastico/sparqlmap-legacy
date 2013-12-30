package org.aksw.sparqlmap.bsbmtestcases;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.apache.commons.collections.MultiHashMap;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.WebContent;
import org.junit.Before;
import org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import com.hp.hpl.jena.query.ResultSetRewindable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.resultset.ResultSetCompare;
import com.hp.hpl.jena.sparql.resultset.ResultSetMem;

public class BSBMComparingPostgresBaseTest {
	
	private Logger log = LoggerFactory.getLogger(this.getClass());
	private SparqlMap sparqlMap;

	@Before
	public void setup(){
		String pathToConf = "./src/main/resources/bsbm-test";
		
		ApplicationContext con = ContextSetup.contextFromFolder(pathToConf);
		sparqlMap = (SparqlMap) con.getBean("sparqlMap");
	}
	
	public void executeAndCompare(String query) throws SQLException{
		
		
		Query queryObject = QueryFactory.create(query);

		QueryExecution qe = QueryExecutionFactory
				.sparqlService("http://localhost:8890/sparql", queryObject,
						"http://bsbm/100k");
			
		if (queryObject.isSelectType()) {
			
			ResultSetRewindable rsSparqlMap = new ResultSetMem(sparqlMap.rewriteAndExecute(query));

			ResultSetRewindable rsVirt = new ResultSetMem(qe.execSelect());

			if(ResultSetCompare.equalsByTerm(rsSparqlMap, rsVirt)){
				assertTrue(true);
			}else{
				rsSparqlMap.reset();
				rsVirt.reset();
				log.warn(" SparqlMap result: \n: "
						+ ResultSetFormatter.asText(rsSparqlMap));
				log.warn("Virtuoso result: \n "
						+ ResultSetFormatter.asText(rsVirt));
				assertTrue(false);
			}
			

//			List<String> vars = rsSparqlMap.getResultVars();
//
//			Multimap<String, RDFNode> smResults = HashMultimap.create();
//			Multimap<String, RDFNode> virtResults = HashMultimap.create();
//
//			while (rsSparqlMap.hasNext()) {
//				assertTrue("SparqlMap has too many results", rsVirt.hasNext());
//				QuerySolution solSm = rsSparqlMap.next();
//				QuerySolution solVirt = rsVirt.next();
//
//				for (String var : vars) {
//					smResults.put(var, solSm.get(var));
//					virtResults.put(var, solVirt.get(var));
//				}
//
//			}
//			assertFalse("Virtuoso had more results", rsVirt.hasNext());
//
//			compareMaps(vars, smResults, virtResults);
//			compareMaps(vars, virtResults,smResults);
		} else if (queryObject.isConstructType()) {
			
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			sparqlMap.executeSparql(query, WebContent.contentTypeTurtle,bos);
			Model smRes = ModelFactory.createDefaultModel();
			
			log.info("Content from SparqlMap construct: " + bos.toString() );
			
			RDFDataMgr.read(smRes, new  ByteArrayInputStream(bos.toByteArray()), Lang.TTL);
			Model virtRes = qe.execConstruct();
			
			
			assertTrue(virtRes.isIsomorphicWith(smRes));
		} else if(queryObject.isDescribeType()){
			
			
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			sparqlMap.executeSparql(query, WebContent.contentTypeTurtle,bos);
			Model smRes = ModelFactory.createDefaultModel();
			
			log.info("Content from SparqlMap construct: " + bos.toString() );
			
			RDFDataMgr.read(smRes, new  ByteArrayInputStream(bos.toByteArray()), Lang.TTL);
			Model virtRes = qe.execDescribe();
			
			
			assertTrue(virtRes.isIsomorphicWith(smRes));
			
		}
		
		

	}

	public void compareMaps(List<String> vars, Multimap<String, RDFNode> map1,
			Multimap<String, RDFNode> map2) {
		for (String var : vars) {
			Collection<RDFNode> map1Nodes = map1.get(var);

			for (RDFNode map1Node : map1Nodes) {
				if (map1Node != null) {
					boolean contains = false;
					for (RDFNode map2Node : map2.get(var)) {
						if (map2Node != null) {
							if (map1Node.isLiteral() && map2Node.isLiteral()) {
								if (map1Node
										.asLiteral()
										.getValue()
										.equals(map2Node.asLiteral().getValue())
										&& (map1Node.asLiteral().getDatatype() == null
												&& map2Node.asLiteral()
														.getDatatype() == null || (map1Node
												.asLiteral().getDatatype() != null
												&& map2Node.asLiteral()
														.getDatatype() != null && map1Node
												.asLiteral()
												.getDatatype()
												.equals(map2Node.asLiteral()
														.getDatatype())))) {
									contains = true;
									break;

								}
							} else {

								contains = map1Node.equals(map2Node);
								if (contains) {
									break;
								}

							}
						}
					}
					assertTrue(
							"Virtuso result did not contain: "
									+ map1Node.toString(), contains);
				}
			}
		}
	}

}
