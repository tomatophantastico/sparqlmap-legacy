package org.aksw.sparqlmap.bsbmtestcases;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.List;

import org.aksw.sparqlmap.core.SparqlMap;
import org.aksw.sparqlmap.core.spring.ContextSetup;
import org.apache.commons.collections.MultiHashMap;
import org.apache.jena.atlas.logging.Log;
import org.junit.Before;
import org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class BSBMComparingPostgresBaseTest {
	
	private Logger log = LoggerFactory.getLogger(this.getClass());
	private SparqlMap sparqlMap;

	@Before
	public void setup(){
		String pathToConf = "./src/main/resources/bsbm-test";
		
		ApplicationContext con = ContextSetup.contextFromFolder(pathToConf);
		sparqlMap = (SparqlMap) con.getBean("sparqlMap");
	}
	
	public void executeAndCompare(String query){
		
		try {
			ResultSet rsSparqlMap = sparqlMap.rewriteAndExecute(query);
			
			ResultSet rsVirt = QueryExecutionFactory.sparqlService("http://localhost:8890/sparql", query, "http://bsbm/100k").execSelect();
			
			log.info(" SparqlMap result: \n: " +ResultSetFormatter.asText(sparqlMap.rewriteAndExecute(query)));
			log.info("Virtuoso result: \n " + ResultSetFormatter.asText(QueryExecutionFactory.sparqlService("http://localhost:8890/sparql", query, "http://bsbm/100k").execSelect()));
			
			List<String> vars = rsSparqlMap.getResultVars();
			
			Multimap<String, RDFNode> smResults = HashMultimap.create();
			Multimap<String, RDFNode> virtResults = HashMultimap.create();
			
			while(rsSparqlMap.hasNext()){
				assertTrue("SparqlMap has too many results", rsVirt.hasNext());
				QuerySolution solSm =  rsSparqlMap.next();
				QuerySolution solVirt = rsVirt.next();
				
				for(String var : vars){
					smResults.put(var, solSm.get(var));
					virtResults.put(var,solVirt.get(var));
				}
				
			}
			assertFalse("Virtuoso had more results",rsVirt.hasNext());
			
			for(String var:vars){
				for(RDFNode smNode: smResults.get(var)){
					boolean contains = false;
					for(RDFNode virtNode : virtResults.get(var)){
						if(smNode.isLiteral() && virtNode.isLiteral()){
							if(smNode.asLiteral().getValue().equals(virtNode.asLiteral().getValue())
								&& (	smNode.asLiteral().getDatatype()==null && virtNode.asLiteral().getDatatype()==null
										||(smNode.asLiteral().getDatatype()!=null && virtNode.asLiteral().getDatatype()!=null
										&& smNode.asLiteral().getDatatype().equals(virtNode.asLiteral().getDatatype())))){
								contains = true;
								break;	
									
							}
						}else{
							contains = smNode.equals(virtNode);
							if(contains){
								break;
							}
						}
					}
					assertTrue("Virtuso result did not contain: " + smNode.toString(), contains);
				}
			}
		} catch (SQLException e) {
			log.error("Error doing the test",e);
		}
		
		

	}

}
